import abc
import hashlib
import json
import re
import time
from datetime import datetime

import six
from oslo_log import log as logging
from oslo_utils import strutils

from DSpace import exception as exc
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.tools.prometheus import PrometheusTool
from DSpace.utils.mail import alert_rule_translation
from DSpace.utils.mail import send_mail

logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class AlertNotifyHelper(object):
    id = None

    def __init__(self, id):
        self.id = id

    @abc.abstractmethod
    def notify(self, msg):
        pass


class EmailHelper(AlertNotifyHelper):
    _config = None

    def __init__(self, id, config):
        super(EmailHelper, self).__init__(id)
        self._config = config

    def notify(self, re_msg):
        """Notify to email"""
        logger.info('Emailhelper will handle msg:%s, mail_conf:%s', re_msg,
                    self._config)
        ctxt = re_msg['ctxt']
        msg = re_msg['msg']
        rule = re_msg['rule']
        alert_rule = objects.AlertRule.get_by_id(
            ctxt, rule.id, expected_attrs=['alert_groups'])
        al_groups = alert_rule.alert_groups
        for al_group in al_groups:
            al_group = objects.AlertGroup.get_by_id(
                ctxt, al_group.id, expected_attrs=['email_groups'])
            email_groups = al_group.email_groups
            for email_group in email_groups:
                receivers = email_group.emails.strip().split(',')
                logger.info('to_emails is:%s', receivers)
                # send per email
                self._per_send_alert_email(
                    rule, msg, self._config, receivers)

    def _per_send_alert_email(self, rule, msg, mail_conf, receivers):
        for receiver in receivers:
            subject_conf = _('alert notify: {}').format(
                alert_rule_translation.get(rule.type))
            content_conf = msg
            mail_conf.update({'smtp_name': _('alert center'),
                              'smtp_to_email': receiver})
            logger.info('begin send email, to_email=%s', receiver)
            try:
                send_mail(subject_conf, content_conf,
                          mail_conf)
                logger.info('send email success,to_email=%s', receiver)
            except Exception as e:
                logger.exception(
                    'send email error,to_email=%s,%s', receiver, str(e))


class WebSocketHelper(AlertNotifyHelper):
    _config = None

    def __init__(self, id, config):
        super(WebSocketHelper, self).__init__(id)
        self._config = config

    def notify(self, re_msg):
        """Notify to websocket"""
        logger.info('Websockethelper will handle msg:%s', re_msg)
        ctxt, msg = re_msg['ctxt'], re_msg['msg']
        resource_obj = re_msg['resource_obj']
        wb = WebSocketClientManager(context=ctxt)
        wb.send_message(ctxt, resource_obj, 'ALERT', msg)


class AlertLogHelper(object):

    def __init__(self, ctxt, cluster_id, rule, resu_metric, current_value):
        self.ctxt = ctxt
        self.cluster_id = cluster_id
        self.rule = rule
        self.resu_metric = resu_metric
        self.current_value = '{:.2%}'.format(current_value)

    @property
    def trigger_value(self):
        return '{:.0%}'.format(float(self.rule.trigger_value))

    @property
    def level(self):
        return self.rule.level

    def notify(self, msg):
        """Notify to Alert Log"""
        pass

    @property
    def resource_type(self):
        return self.rule.resource_type

    @property
    def rule_type(self):
        return self.rule.type

    def handled_msg(self):
        resu_data = self.add_an_alert_log()
        if not resu_data:
            return None
        return {
            'ctxt': self.ctxt,
            'cluster_id': self.cluster_id,
            'msg': resu_data.get('alert_value'),
            'rule': self.rule,
            'resource_obj': resu_data.get('resource_obj')
        }

    def add_an_alert_log(self):
        if self.resource_type == 'cluster':
            resu_data = self.collect_cluster_log()
        elif self.resource_type == 'node':
            resu_data = self.collect_node_log()
        elif self.resource_type == 'disk':
            resu_data = self.collect_disk_log()
        elif self.resource_type == 'pool':
            resu_data = self.collect_pool_log()
        elif self.resource_type == 'osd':
            resu_data = self.collect_osd_log()
        elif self.resource_type == 'network_interface':
            resu_data = self.collect_network_interface_log()
        else:
            resu_data = None
        if resu_data:
            resu_obj = resu_data.pop('resource_obj')
            log_data = {
                'resource_type': self.resource_type,
                'level': self.level,
                'alert_rule_id': self.rule.id,
                'cluster_id': self.cluster_id
            }
            resu_data.update(log_data)
            alert_log = objects.AlertLog(self.ctxt, **resu_data)
            alert_log.create()
            logger.info('add an alert_log success, resource_type: %s, '
                        'resource_name: %s, alert_value: %s',
                        resu_data['resource_type'],
                        resu_data['resource_name'],
                        resu_data['alert_value'])
            resu_data.update({'resource_obj': resu_obj})
        return resu_data

    def is_exist_node(self, hostname):
        node = objects.NodeList.get_all(
            self.ctxt, filters={'hostname': hostname,
                                'cluster_id': self.cluster_id})
        if not node:
            logger.warning('hostname: %s not found', hostname)
            return None
        return node[0]

    def is_exist_disk(self, name, node):
        disk = objects.DiskList.get_all(
            self.ctxt, filters={'name': name, 'cluster_id': self.cluster_id,
                                'node_id': node.id})
        if not disk:
            logger.warning('disk_name: %s not found', name)
            return None
        return disk[0]

    def is_exist_pool(self, pool_id):
        pool = objects.PoolList.get_all(
            self.ctxt, filters={'pool_id': pool_id,
                                'cluster_id': self.cluster_id})
        if not pool:
            logger.warning('pool_id: %s not found', pool_id)
            return None
        return pool[0]

    def is_exist_osd(self, osd_id):
        osd = objects.OsdList.get_all(
            self.ctxt, filters={'osd_id': osd_id,
                                'cluster_id': self.cluster_id})
        if not osd:
            logger.warning('osd_id: %s not found', osd_id)
            return None
        return osd[0]

    def is_exist_network(self, name, node):
        network = objects.NetworkList.get_all(
            self.ctxt, filters={'name': name, 'cluster_id': self.cluster_id,
                                'node_id': node.id})
        if not network:
            logger.warning('network_name: %s not found', name)
            return None
        return network[0]

    def collect_cluster_log(self):
        resource_id = self.cluster_id
        cluster = objects.Cluster.get_by_id(self.ctxt, self.cluster_id)
        if not cluster:
            logger.warning('cluster_id: %s not found', resource_id)
            return None
        if self.rule_type == 'cluster_usage':
            alert_value = _("Cluster capacity usage is above {}, (current "
                            "value is: {})").format(self.trigger_value,
                                                    self.current_value)
        else:
            return None
        return {
            'resource_id': resource_id,
            'resource_name': cluster.display_name,
            'alert_value': alert_value,
            'resource_obj': cluster
        }

    def collect_node_log(self):
        resource_name = self.resu_metric.get('hostname')
        node = self.is_exist_node(resource_name)
        if not node:
            return None
        if self.rule_type == 'cpu_usage':
            alert_value = _("hostname:{} CPU usage is above {} (current value "
                            "is: {})").format(resource_name,
                                              self.trigger_value,
                                              self.current_value)
        elif self.rule_type == 'memory_usage':
            alert_value = _("hostname:{} Memory usage is above {} (current "
                            "value is: {})").format(resource_name,
                                                    self.trigger_value,
                                                    self.current_value)
        elif self.rule_type == 'sys_disk_usage':
            alert_value = _("hostname:{} Sys disk usage is above {} (current "
                            "value is: {})").format(resource_name,
                                                    self.trigger_value,
                                                    self.current_value)
        else:
            return None
        return {
            'resource_id': node.id,
            'resource_name': resource_name,
            'alert_value': alert_value,
            'resource_obj': node
        }

    def collect_disk_log(self):
        hostname = self.resu_metric.get('hostname')
        resource_name = self.resu_metric.get('device')
        node = self.is_exist_node(hostname)
        if not node:
            return None
        disk = self.is_exist_disk(resource_name, node)
        if not disk:
            return None
        if self.rule_type == 'disk_usage':
            alert_value = _("hostname: {}, drive_letter: {} Disk usage is "
                            "above {} (current value is {})"
                            ).format(node.hostname, resource_name,
                                     self.trigger_value, self.current_value)
        else:
            return None
        return {
            'resource_id': node.id,
            'resource_name': resource_name,
            'alert_value': alert_value,
            'resource_obj': disk
        }

    def collect_pool_log(self):
        pool_id = self.resu_metric.get('pool_id')
        pool = self.is_exist_pool(pool_id)
        if not pool:
            return None
        display_name = pool.display_name
        if self.rule_type == 'pool_usage':
            alert_value = _("pool_name: {}, Pool capacity usage is above {} "
                            "(current value is: {})"
                            ).format(display_name, self.trigger_value,
                                     self.current_value)
        else:
            return None
        return {
            'resource_id': pool.id,
            'resource_name': display_name,
            'alert_value': alert_value,
            'resource_obj': pool
        }

    def collect_osd_log(self):
        hostname = self.resu_metric.get('hostname')
        osd_id = self.resu_metric.get('osd_id')
        node = self.is_exist_node(hostname)
        if not node:
            return None
        osd = self.is_exist_osd(osd_id)
        if not osd:
            return None
        osd_name = osd.osd_name
        if self.rule_type == 'osd_usage':
            alert_value = _("hostname: {}, osd_name: {}, Osd capacity usage "
                            "is above {} (current value is: {})"
                            ).format(node.hostname, osd_name,
                                     self.trigger_value,
                                     self.current_value)
        else:
            return None
        return {
            'resource_id': osd.id,
            'resource_name': osd_name,
            'alert_value': alert_value,
            'resource_obj': osd
        }

    def collect_network_interface_log(self):
        hostname = self.resu_metric.get('hostname')
        node = self.is_exist_node(hostname)
        if not node:
            return None
        net_name = self.resu_metric.get('device')
        net = self.is_exist_network(net_name, node)
        if not net:
            return None
        if self.rule_type == 'transmit_bandwidth_usage':
            alert_value = _("hostname: {}, network_name: {}, transmit "
                            "bandwidth usage is above {} (current value is: "
                            "{})").format(node.hostname, net_name,
                                          self.trigger_value,
                                          self.current_value)
        elif self.rule_type == 'receive_bandwidth_usage':
            alert_value = _("hostname: {}, network_name: {}, receive "
                            "bandwidth usage is above {} (current value is: "
                            "{})").format(node.hostname, net_name,
                                          self.trigger_value,
                                          self.current_value)
        else:
            return None
        return {
            'resource_id': net.id,
            'resource_name': net_name,
            'alert_value': alert_value,
            'resource_obj': net
        }


class AlertNotifyGroup(object):
    _helpers = None

    def __init__(self):
        self._helpers = {}

    def notify(self, msg):
        for helper in six.itervalues(self._helpers):
            helper.notify(msg)

    def append(self, helper):
        logger.info("AlertNotifyGroup append helper %s", helper)
        self._helpers[helper.id] = helper

    def remove(self, helper_id):
        logger.info("AlertNotifyGroup remove helper %s", helper_id)
        self._helpers.pop(helper_id, None)


class AlertQuery(object):
    id = None
    _watcher = None
    _checks = None

    def __init__(self, id):
        self.id = id

    def set_watcher(self, watcher):
        self._watcher = watcher
        self._checks = {}

    def check(self):
        pass

    def append_check(self, cluster_id, func):
        logger.info("AlertQuery append check %s", cluster_id)
        self._checks[cluster_id] = func

    def remove_check(self, cluster_id):
        logger.info("AlertQuery remove check %s", cluster_id)
        self._checks.pop(cluster_id)

    def send_alert(self, cluster_id, msg):
        notifys = self._watcher.get_notify(cluster_id)
        if notifys:
            notifys.notify(msg)

    def get_check(self, cluster_id):
        return self._checks[cluster_id]


class PrometheusQuery(AlertQuery):
    promql = None
    _checks = None

    def __init__(self, id, promql, prome_client):
        super(PrometheusQuery, self).__init__(id)
        self.promql = promql
        self.prome_client = prome_client
        self.last_times = {}

    @property
    def ctxt(self):
        return RequestContext(user_id='admin', is_admin=False)

    def check(self):
        """Do prometheus query and send alert"""
        results = self.prome_client.prometheus_get_metrics(self.promql)
        if not results:
            logger.warning('prometheus_results is None, promql: %s',
                           self.promql)
            return
        else:
            logger.debug('promql: %s, results: %s', self.promql, results)
            for resu in results:
                cluster_id = resu['metric']['cluster_id']
                check_fun = self.get_check(cluster_id)
                msg = check_fun(resu)
                if msg:
                    logger.info('promql: %s, handled msg: %s', self.promql,
                                msg)
                    self.send_alert(cluster_id, msg)

    def check_fun(self, resu):
        # 每条result对比结果
        logger.debug('one result: %s', resu)
        rule_type = self.id
        cluster_id = resu['metric']['cluster_id']
        current_value = float(resu['value'][1])
        rule = objects.AlertRuleList.get_all(
            self.ctxt, filters={'cluster_id': cluster_id, 'type': rule_type,
                                'enabled': True})
        if rule:
            rule = rule[0]
            logger.debug('rule_type:%s', rule_type)
            if rule_type in ['transmit_bandwidth_usage',
                             'receive_bandwidth_usage']:
                # 具体到每个网卡:发送/接受带宽使用率单独计算
                logger.debug('rule_type_net:%s', rule_type)
                resu_metric = resu['metric']
                current_value = self.per_network_bandwidth(
                    cluster_id, resu_metric, current_value)
                logger.info('network handled value:%s', current_value)
                if not current_value:
                    return None
            trigger_value = float(rule.trigger_value)
            trigger_period = int(rule.trigger_period) * 60
            if current_value > trigger_value:
                # 1、阈值达标
                metric_md5 = hashlib.md5(json.dumps(resu['metric']).encode(
                    'utf-8')).hexdigest()
                logger.info('all_resus_last_times is: %s', self.last_times)
                time_now = datetime.now()
                if metric_md5 not in self.last_times:
                    # 首次记录
                    self.last_times[metric_md5] = time_now
                    # 1、 add a AlertLogHelper()
                    per_log = AlertLogHelper(
                        self.ctxt, cluster_id, rule, resu['metric'],
                        current_value)
                    msg = per_log.handled_msg()
                else:
                    last_time = self.last_times[metric_md5]
                    time_diff = (time_now - last_time).total_seconds()
                    if time_diff > trigger_period:
                        self.last_times[metric_md5] = time_now
                        # 更新记录
                        per_log = AlertLogHelper(
                            self.ctxt, cluster_id, rule, resu['metric'],
                            current_value)
                        msg = per_log.handled_msg()
                    else:
                        # 告警周期未达到
                        logger.info('alert_rule: %s, time has not more than '
                                    '%sm, cluster_id: %s', rule_type,
                                    trigger_period/60, cluster_id)
                        msg = None
            else:
                # 阈值未达标
                logger.info('alert_rule: %s, value not more than %s, '
                            'cluster_id: %s', rule_type, trigger_value,
                            cluster_id)
                msg = None
        else:
            # 未找到开启的告警规则
            logger.info('alert_rule:%s not found or unabled, cluster_id: %s',
                        rule_type, cluster_id)
            msg = None
        return msg

    def per_network_bandwidth(self, cluster_id, resu_metric, current_value):
        hostname = resu_metric.get('hostname')
        node = objects.NodeList.get_all(
            self.ctxt, filters={'hostname': hostname,
                                'cluster_id': cluster_id})
        if not node:
            logger.warning('hostname:%s not found,cluster_id:%s', hostname,
                           cluster_id)
            return None
        node = node[0]
        net_name = resu_metric.get('device')
        net = objects.NetworkList.get_all(
            self.ctxt, filters={'cluster_id': cluster_id, 'node_id': node.id,
                                'name': net_name})
        if not net:
            logger.warning('network_name:%s not found, hostname:%s, '
                           'cluster_id:%s', net_name, hostname, cluster_id)
            return None
        net = net[0]
        speed = net.speed
        # 正则匹配 '10000Mb/s'
        speed_num = re.match(r"\d+", speed)
        if not speed_num:
            logger.warning('network_name:%s(%s) speed_value is not int',
                           net_name, hostname)
            return None
        speed_num = speed_num.group()
        speed_unit = speed.split(speed_num)[1]
        if speed_unit == 'Mb/s':
            _speed = (int(speed_num)/8) * 1024 * 1024
        else:
            logger.warning('network_name:%s(%s) speed_unit is invalid',
                           net_name, hostname)
            return None
        value = current_value/_speed
        logger.info('handled network_bandwidth:%s(%s) value is %s', net_name,
                    hostname, value)
        return float(value)


class AlertWatcher(object):
    _querys = None
    _notifys = None

    def __init__(self):
        self._querys = {}
        self._notifys = {}

    def check(self, interval):
        """Loop for all query"""
        while True:
            for query in six.itervalues(self._querys):
                query.check()
            time.sleep(interval)

    def append_query(self, query):
        """Append a new query"""
        logger.info("AlertWatcher append query %s", query)
        self._querys[query.id] = query
        query.set_watcher(self)

    def remove_query(self, query_id):
        """Remove a query"""
        logger.info("AlertWatcher remove query %s", query_id)
        self._querys.pop(query_id)

    def get_query(self, query_id):
        if query_id in self._querys:
            return self._querys[query_id]
        else:
            return None

    def append_notify(self, cluster_id, notify_group):
        """Append AlertNotifyGroup for a cluster"""
        logger.info("AlertWatcher append cluster_id(%s) notify_group %s",
                    cluster_id, notify_group)
        self._notifys[cluster_id] = notify_group

    def remove_notify(self, cluster_id):
        """Remove AlertNotifyGroup for a cluster"""
        logger.info("AlertWatcher remove cluster_id(%s) notify_group %s",
                    cluster_id)
        self._notifys.pop(cluster_id, None)

    def get_notify(self, cluster_id):
        if cluster_id not in self._notifys:
            return None
        return self._notifys[cluster_id]


class AlertRuleHandler(AdminBaseHandler):

    def __init__(self, *args, **kwargs):
        super(AlertRuleHandler, self).__init__(*args, **kwargs)

    def bootstrap(self):
        super(AlertRuleHandler, self).bootstrap()
        self._setup_alert_watcher()

    def _setup_alert_watcher(self):
        self.alert_watcher = AlertWatcher()
        # 1. get all cluster prome rules
        rules = self.get_cluster_prome_rules()
        logger.info('all_clusters_rules:%s', rules)
        self.update_prometheus_que(rules)
        clusters = objects.ClusterList.get_all(self.ctxt)
        for cluster in clusters:
            self.update_notify_group(cluster.id)
        self.task_submit(self._alert_watcher_check)
        logger.info('alert_watcher_check task has begin')

    def _alert_watcher_check(self):
        self.wait_ready()
        logger.debug("Start alert watcher check")
        while True:
            self.alert_watcher.check(CONF.alert_rule_check_interval)

    def get_cluster_prome_rules(self, cluster_id=None):
        rules = objects.AlertRuleList.get_all(
            self.ctxt, filters={
                'data_source': 'prometheus', 'cluster_id':
                    cluster_id if cluster_id else '*'})
        return rules

    def update_prometheus_que(self, rules, remove=None):
        rules = rules if rules else []
        prome_tool = PrometheusTool(self.ctxt)
        avg_time = str(CONF.alert_rule_average_time) + 's'
        for rule in rules:
            cluster_id = rule.cluster_id
            que = self.alert_watcher.get_query(rule.type)
            if not que:
                # not prometheus_que, create an new peometheus_que, then append
                promql = rule.query_grammar.format(avg_time=avg_time)
                que = PrometheusQuery(rule.type, promql, prome_tool)
                self.alert_watcher.append_query(que)
            if remove:
                # remove_check(cluster_id) when remove a cluster
                que.remove_check(cluster_id)
            else:
                # append_check when add a cluster
                que.append_check(cluster_id, que.check_fun)

    def update_notify_group(self, cluster_id, remove=None):
        notify_group = self.alert_watcher.get_notify(cluster_id)
        if notify_group:
            # notify_group exist
            if remove:
                # remove_notify when remove a cluster
                self.alert_watcher.remove_notify(cluster_id)
                return
        else:
            # notify_group not exist, add a new notify_group, then append
            notify_group = AlertNotifyGroup()
            self.alert_watcher.append_notify(cluster_id, notify_group)
        # 1. append websocket
        notify_group.append(
            WebSocketHelper('wbsocket_helper', config=None))
        # 2. is or not append email
        smtp = objects.SysConfigList.get_all(
            self.ctxt, filters={'cluster_id': cluster_id,
                                'key': 'smtp_enabled'})
        mail_conf = {}
        if smtp:
            enabled = strutils.bool_from_string(smtp[0].value)
            if enabled:
                smtp_configs = objects.SysConfigList.get_all(
                    self.ctxt, filters={"cluster_id": cluster_id})
                keys = ['smtp_user', 'smtp_password', 'smtp_host',
                        'smtp_port',
                        'smtp_enable_ssl', 'smtp_enable_tls']
                for smtp_conf in smtp_configs:
                    if smtp_conf.key in keys:
                        mail_conf[smtp_conf.key] = smtp_conf.value
                # enable_ssl,enable_tls: str -> bool
                mail_conf['smtp_enable_ssl'] = strutils.bool_from_string(
                    mail_conf['smtp_enable_ssl'])
                mail_conf['smtp_enable_tls'] = strutils.bool_from_string(
                    mail_conf['smtp_enable_tls'])
                notify_group.append(EmailHelper('email_helper', mail_conf))

    def alert_rule_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        return objects.AlertRuleList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_rule_get(self, ctxt, alert_rule_id, expected_attrs=None):
        return objects.AlertRule.get_by_id(ctxt, alert_rule_id, expected_attrs)

    def alert_rule_update(self, ctxt, alert_rule_id, data):
        rule = self.alert_rule_get(ctxt, alert_rule_id)
        enabled = data.get('enabled')
        trigger_value = data.get('trigger_value')
        trigger_period = data.get('trigger_period')
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ALERT_RULE, before_obj=rule)
        if enabled is True:
            rule.enabled = True
            action = AllActionType.OPEN_ALERT_RULE
        elif enabled is False:
            rule.enabled = False
            action = AllActionType.CLOSE_ALERT_RULE
        elif trigger_value and trigger_period:
            rule.trigger_value = trigger_value
            rule.trigger_period = trigger_period
            action = AllActionType.UPDATE
        else:
            raise exc.InvalidInput(_('alert_rule upda param not exist'))
        rule.save()
        self.finish_action(begin_action, resource_id=rule.id,
                           resource_name=rule.type,
                           action=action, after_obj=rule)
        return rule

    def alert_rule_get_count(self, ctxt, filters=None):
        return objects.AlertRuleList.get_count(
            ctxt, filters=filters)


class AlertRuleInitMixin(object):

    def init_alert_rule(self, ctxt, cluster_id):
        init_datas = [
            {
                'resource_type': 'cluster',
                'type': 'cluster_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "ceph_cluster_total_used_bytes/ceph_cluster_total_bytes"
            },
            {
                'resource_type': 'node',
                'type': 'cpu_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "1 - (avg by (hostname,cluster_id) (irate("
                    "node_cpu_seconds_total{{mode='idle'}}[{avg_time}])))"
            },
            {
                'resource_type': 'node',
                'type': 'memory_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.85',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "(avg_over_time(node_memory_MemTotal_bytes[{avg_time}]) - "
                    "avg_over_time(node_memory_MemFree_bytes[{avg_time}])) / "
                    "avg_over_time(node_memory_MemTotal_bytes[{avg_time}])"
            },
            {
                'resource_type': 'node',
                'type': 'sys_disk_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "(node_filesystem_size_bytes{{mountpoint='/',"
                    "device!='rootfs'}} - node_filesystem_free_bytes{{"
                    "mountpoint='/', device!='rootfs'}}) / ("
                    "node_filesystem_size_bytes{{mountpoint='/', "
                    "device!='rootfs'}} - node_filesystem_free_bytes{{"
                    "mountpoint='/', device!='rootfs'}} + "
                    "node_filesystem_avail_bytes{{mountpoint='/', "
                    "device!='rootfs'}})"
            },
            {
                'resource_type': 'osd',
                'type': 'osd_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "ceph_osd_capacity_kb_used/ceph_osd_capacity_kb"
            },
            {
                'resource_type': 'pool',
                'type': 'pool_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "ceph_pool_bytes_used/(ceph_pool_max_avail + "
                    "ceph_pool_bytes_used)"
            },
            {
                'resource_type': 'disk',
                'type': 'disk_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "irate(node_disk_io_time_seconds_total{{device!~'dm.*'}}"
                    "[{avg_time}])"
            },
            {
                'resource_type': 'network_interface',
                'type': 'transmit_bandwidth_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "irate(node_network_transmit_bytes_total{{}}[{avg_time}])"
            },
            {
                'resource_type': 'network_interface',
                'type': 'receive_bandwidth_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus',
                'query_grammar':
                    "irate(node_network_receive_bytes_total{{}}[{avg_time}])"
            },
            {
                'resource_type': 'disk',
                'type': 'disk_online',
                'trigger_mode': 'eq',
                'trigger_value': 'online',
                'level': 'INFO',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace',
            },
            {
                'resource_type': 'disk',
                'type': 'disk_offline',
                'trigger_mode': 'eq',
                'trigger_value': 'offline',
                'level': 'ERROR',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace',
            },
            {
                'resource_type': 'service',
                'type': 'service_status',
                'trigger_mode': 'eq',
                'trigger_value': 'inactive',
                'level': 'WARN',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace',
            },
            {
                'resource_type': 'service',
                'type': 'service_status',
                'trigger_mode': 'eq',
                'trigger_value': 'error',
                'level': 'ERROR',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
            },
            {
                'resource_type': 'service',
                'type': 'service_status',
                'trigger_mode': 'eq',
                'trigger_value': 'active',
                'level': 'INFO',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
            },
            {
                'resource_type': 'osd',
                'type': 'osd_status',
                'trigger_mode': 'eq',
                'trigger_value': 'offline',
                'level': 'WARN',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
            },
            {
                'resource_type': 'osd',
                'type': 'osd_status',
                'trigger_mode': 'eq',
                'trigger_value': 'error',
                'level': 'ERROR',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
            },
            {
                'resource_type': 'osd',
                'type': 'osd_status',
                'trigger_mode': 'eq',
                'trigger_value': 'active',
                'level': 'INFO',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
            },
        ]

        for init_data in init_datas:
            init_data.update({'cluster_id': cluster_id})
            alert_rule = objects.AlertRule(ctxt, **init_data)
            alert_rule.create()
