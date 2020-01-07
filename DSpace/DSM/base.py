import json
import time
from concurrent import futures

from oslo_log import log as logging
from oslo_utils import strutils
from oslo_utils import timeutils

from DSpace import context
from DSpace import exception as exc
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSA.client import AgentClientManager
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.base import StorObject
from DSpace.objects.fields import ConfigKey
from DSpace.objects.fields import DSMStatus
from DSpace.taskflows.base import task_manager
from DSpace.utils.mail import alert_rule_translation
from DSpace.utils.mail import mail_template
from DSpace.utils.mail import send_mail
from DSpace.utils.service_map import ServiceMap

logger = logging.getLogger(__name__)


class AdminBaseHandler(object):
    slow_requests = {}

    def __init__(self):
        self.status = DSMStatus.INIT
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)
        ctxt = context.get_context(user_id="admin")
        self.debug_mode = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DEBUG_MODE)
        self.container_namespace = objects.sysconfig.sys_config_get(
            ctxt, "image_namespace")
        self.map_util = ServiceMap(self.container_namespace)
        self.bootstrap(ctxt)

    def bootstrap(self, ctxt):
        self._setup_agent_manager(ctxt)
        clusters = objects.ClusterList.get_all(ctxt)
        for cluster in clusters:
            ctxt = context.get_context(cluster.id, user_id="admin")
            self._add_node_to_agent_manager(ctxt)
            self._clean_taskflow(ctxt)

    def _setup_agent_manager(self, ctxt):
        agent_port = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.AGENT_PORT)
        agent_manager = AgentClientManager(ctxt, agent_port)
        self.agent_manager = agent_manager
        setattr(context, 'agent_manager', agent_manager)

    def _add_node_to_agent_manager(self, ctxt):
        nodes = objects.NodeList.get_all(
            ctxt, filters={"status": s_fields.NodeStatus.ALIVE})
        logger.info("Node get all %s", nodes)
        for node in nodes:
            logger.info("add node to agent manager %s", node)
            self.agent_manager.add_node(node)

    def _clean_taskflow(self, ctxt):
        task_manager.bootstrap(ctxt)

    def is_ready(self):
        return self.status == DSMStatus.ACTIVE

    def wait_ready(self):
        while True:
            if self.is_ready():
                break
            time.sleep(1)

    def to_active(self):
        self.status = DSMStatus.ACTIVE

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

    def begin_action(self, ctxt, resource_type=None, action=None,
                     before_obj=None):
        logger.debug('begin action, resource_type:%s, action:%s',
                     resource_type, action)
        if isinstance(before_obj, StorObject):
            before_data = json.dumps(before_obj.to_dict())
        else:
            before_data = json.dumps(before_obj)
        action_data = {
            'begin_time': timeutils.utcnow(),
            'client_ip': ctxt.client_ip,
            'user_id': ctxt.user_id,
            'resource_type': resource_type,
            'action': action,
            'before_data': (before_data if before_data else None),
            'cluster_id': ctxt.cluster_id
        }
        action_log = objects.ActionLog(ctxt, **action_data)
        action_log.create()
        return action_log

    def finish_action(self, begin_action=None, resource_id=None,
                      resource_name=None, after_obj=None, status=None,
                      action=None, err_msg=None, diff_data=None,
                      *args, **kwargs):
        begin_action.finish_action(
            resource_id=resource_id, resource_name=resource_name,
            after_obj=after_obj, status=status, action=action,
            err_msg=err_msg, diff_data=diff_data,
            *args, **kwargs)

    def get_ceph_cluster_status(self, ctxt):
        cluster = objects.Cluster.get_by_id(ctxt, ctxt.cluster_id)
        return cluster.ceph_status

    def has_monitor_host(self, ctxt):
        cluster_id = ctxt.cluster_id
        mon_host = self.monitor_count(ctxt)
        if not mon_host:
            logger.warning('cluster {} has no active mon host'.format(
                cluster_id))
            return False
        if not self.get_ceph_cluster_status(ctxt):
            logger.warning('Could not connect to ceph cluster {}'.format(
                cluster_id))
            return False
        filters = {'status': s_fields.ServiceStatus.ACTIVE, 'name': 'MON'}
        services = objects.ServiceList.get_count(ctxt, filters=filters)
        if services / mon_host > 0.5:
            return True
        logger.warning('cluster {} has no enough active mon host'.format(
            cluster_id))
        return False

    def is_agent_available(self, ctxt, node_id):
        agents = objects.ServiceList.get_all(ctxt, filters={
            'status':  s_fields.ServiceStatus.ACTIVE,
            'name': "DSA",
            'node_id': node_id
        })
        if agents:
            return True
        else:
            return False

    def check_agent_available(self, ctxt, node):
        if not self.is_agent_available(ctxt, node.id):
            raise exc.InvalidInput(_("DSA service in node(%s) not available"
                                     ) % node.hostname)

    def monitor_count(self, ctxt):
        filters = {'role_monitor': True}
        mon_host = objects.NodeList.get_count(ctxt, filters=filters)
        if not mon_host:
            return 0
        return mon_host

    def check_mon_host(self, ctxt):
        mon_host = self.monitor_count(ctxt)
        if not mon_host:
            logger.warning('cluster {} no monitor role'.format(
                ctxt.cluster_id))
            raise exc.ClusterNoMonitorRole()

        if not self.get_ceph_cluster_status(ctxt):
            logger.warning('Could not connect to ceph cluster {}'.format(
                ctxt.cluster_id))
            raise exc.ClusterNotHealth()

        filters = {'status': s_fields.ServiceStatus.ACTIVE, 'name': 'MON'}
        services = objects.ServiceList.get_count(ctxt, filters=filters)
        if services / mon_host <= 0.5:
            logger.warning('cluster {} has no enough active mon host'.format(
                self.cluster_id))
            raise exc.ClusterNotHealth()

    def check_service_status(self, ctxt, service):
        time_now = timeutils.utcnow(with_timezone=True)
        if service.updated_at:
            update_time = service.updated_at
        else:
            update_time = service.created_at
        time_diff = time_now - update_time
        if (service.status == s_fields.ServiceStatus.ACTIVE) \
                and (time_diff.total_seconds() > CONF.service_max_interval):
            service.status = s_fields.ServiceStatus.INACTIVE
            service.save()

    def notify_node_update(self, ctxt, node):
        client = self.agent_manager.get_client(node.id)
        client.node_update_infos(ctxt, node)

    def if_service_alert(self, ctxt, check_cluster=True, node=None):
        cluster = objects.Cluster.get_by_id(ctxt, ctxt.cluster_id)
        if check_cluster and cluster.status == s_fields.ClusterStatus.DELETING:
            return False
        if node:
            if node.status in [s_fields.NodeStatus.CREATING,
                               s_fields.NodeStatus.DELETING]:
                return False
        return True

    def send_websocket(self, ctxt, service, wb_op_status, alert_msg):
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, service, wb_op_status, alert_msg)

    def send_service_alert(self, ctxt, service, alert_type, resource_name,
                           alert_level, alert_msg, wb_op_status):
        alert_rule = objects.AlertRuleList.get_all(
            ctxt, filters={
                'type': alert_type, 'cluster_id': ctxt.cluster_id,
                'level': alert_level
            })
        if not alert_rule:
            return
        alert_rule = alert_rule[0]
        if not alert_rule.enabled:
            return
        alert_log_data = {
            'resource_type': alert_rule.resource_type,
            'resource_name': resource_name,
            'resource_id': ctxt.cluster_id,
            'level': alert_rule.level,
            'alert_value': alert_msg,
            'alert_rule_id': alert_rule.id,
            'cluster_id': ctxt.cluster_id
        }
        alert_log = objects.AlertLog(ctxt, **alert_log_data)
        alert_log.create()
        self.send_websocket(ctxt, service, wb_op_status, alert_msg)
        self.send_alert_email(ctxt, alert_rule, alert_msg)

    def get_smtp_conf(self, ctxt):
        cluster_id = ctxt.cluster_id
        smtp = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": cluster_id,
                           'key': 'smtp_enabled'})
        if not smtp:
            logger.info('not yet deploy SMTP conf,'
                        'will not send alert emails')
            return None
        enabled = strutils.bool_from_string(smtp[0].value)
        if not enabled:
            logger.info('SMTP conf has closed, will not send alert emails')
            return None
        mail_conf = {}
        smtp_configs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": cluster_id})
        keys = ['smtp_user', 'smtp_password', 'smtp_host', 'smtp_port',
                'smtp_enable_ssl', 'smtp_enable_tls']
        for smtp_conf in smtp_configs:
            if smtp_conf.key in keys:
                mail_conf[smtp_conf.key] = smtp_conf.value
        # enable_ssl,enable_tls: str -> bool
        mail_conf['smtp_enable_ssl'] = strutils.bool_from_string(
            mail_conf['smtp_enable_ssl'])
        mail_conf['smtp_enable_tls'] = strutils.bool_from_string(
            mail_conf['smtp_enable_tls'])
        return mail_conf

    def send_alert_email(self, ctxt, alert_rule, alert_msg):
        mail_conf = self.get_smtp_conf(ctxt)
        if not mail_conf:
            return
        logger.info('smtp_conf:%s', mail_conf)
        alert_rule = objects.AlertRule.get_by_id(
            ctxt, alert_rule.id, expected_attrs=['alert_groups'])
        al_groups = alert_rule.alert_groups
        for al_group in al_groups:
            al_group = objects.AlertGroup.get_by_id(
                ctxt, al_group.id, expected_attrs=['email_groups'])
            email_groups = al_group.email_groups
            for email_group in email_groups:
                to_emails = email_group.emails.strip().split(',')
                logger.info('to_emails is:%s', to_emails)
                # send per email
                self._per_send_alert_email(
                    alert_rule, alert_msg, mail_conf, to_emails)

    def _per_send_alert_email(self, alert_rule, alert_msg,
                              mail_conf, to_emails):
        for to_email in to_emails:
            subject_conf = {
                'smtp_subject': _('alert notify: {}').format(
                    alert_rule_translation.get(alert_rule.type))}
            content_conf = {'smtp_content': mail_template(alert_msg=alert_msg)}
            mail_conf.update({'smtp_name': _('alert center'),
                              'smtp_to_email': to_email})
            logger.info('begin send email, to_email=%s', to_email)
            try:
                send_mail(subject_conf, content_conf,
                          mail_conf)
                logger.info('send email success,to_email=%s', to_email)
            except Exception as e:
                logger.error(
                    'send email error,to_email=%s,%s', to_email, str(e))
