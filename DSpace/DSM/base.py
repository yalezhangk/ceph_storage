import json
from concurrent import futures

from oslo_log import log as logging
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
from DSpace.utils.service_map import ServiceMap

logger = logging.getLogger(__name__)


class AdminBaseHandler(object):
    slow_requests = {}

    def __init__(self):
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)
        ctxt = context.get_context()
        self.debug_mode = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DEBUG_MODE)
        self.container_namespace = objects.sysconfig.sys_config_get(
            ctxt, "image_namespace")
        self.map_util = ServiceMap(self.container_namespace)

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
        if isinstance(after_obj, StorObject):
            after_data = json.dumps(after_obj.to_dict())
        else:
            after_data = json.dumps(after_obj)
        finish_data = {
            'resource_id': resource_id,
            'resource_name': resource_name,
            'after_data': (after_data if after_data else None),
            'status': 'success',
            'finish_time': timeutils.utcnow(),
            'err_msg': err_msg,
            'diff_data': diff_data
        }
        if action:
            finish_data.update({'action': action})
        if status:
            if status in ['active', 'success', 'available']:
                finish_data.update({'status': 'success'})
            else:
                finish_data.update({'status': 'fail'})
        begin_action.update(finish_data)
        logger.debug('finish action, resource_name:%s, action:%s, status:%s',
                     resource_name, action, finish_data['status'])
        begin_action.save()

    def get_ceph_cluster_status(self, ctxt):
        cluster = objects.Cluster.get_by_id(ctxt, ctxt.cluster_id)
        return cluster.ceph_status

    def has_monitor_host(self, ctxt):
        cluster_id = ctxt.cluster_id
        filters = {'role_monitor': True}
        mon_host = objects.NodeList.get_count(ctxt, filters=filters)
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

    def check_mon_host(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if not has_mon_host:
            raise exc.InvalidInput(
                reason=_('has not active mon host, can not do any '
                         'ceph actions'))

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
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id).get_client(node.id)
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
