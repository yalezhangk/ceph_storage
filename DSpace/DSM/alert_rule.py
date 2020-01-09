from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType

logger = logging.getLogger(__name__)


class AlertRuleHandler(AdminBaseHandler):
    def alert_rule_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        return objects.AlertRuleList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_rule_get(self, ctxt, alert_rule_id, expected_attrs=None):
        return objects.AlertRule.get_by_id(ctxt, alert_rule_id, expected_attrs)

    def alert_rule_update(self, ctxt, alert_rule_id, data):
        rule = self.alert_rule_get(ctxt, alert_rule_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ALERT_RULE,
            action=AllActionType.OPEN_ALERT_RULE, before_obj=rule)
        rule_data = {
            'enabled': data.get('enabled')
        }
        rule.update(rule_data)
        rule.save()
        if rule_data['enabled']:
            action = AllActionType.OPEN_ALERT_RULE
        else:
            action = AllActionType.CLOSE_ALERT_RULE
        self.finish_action(begin_action, resource_id=rule.id,
                           resource_name=rule.type,
                           resource_data=objects.json_encode(rule),
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
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'node',
                'type': 'cpu_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'node',
                'type': 'memory_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.85',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'node',
                'type': 'sys_disk_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'osd',
                'type': 'osd_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'pool',
                'type': 'pool_usage',
                'trigger_mode': 'gt',
                'trigger_value': '0.8',
                'level': 'WARN',
                'trigger_period': '1440',
                'data_source': 'prometheus'
            },
            {
                'resource_type': 'disk',
                'type': 'disk_online',
                'trigger_mode': 'eq',
                'trigger_value': 'online',
                'level': 'INFO',
                'trigger_period': '0',
                'data_source': 'dspace'
            },
            {
                'resource_type': 'disk',
                'type': 'disk_offline',
                'trigger_mode': 'eq',
                'trigger_value': 'offline',
                'level': 'ERROR',
                'trigger_period': '0',
                'data_source': 'dspace'
            },
            {
                'resource_type': 'service',
                'type': 'service_status',
                'trigger_mode': 'eq',
                'trigger_value': 'inactive',
                'level': 'WARN',
                'trigger_period': '0',
                'enabled': True,
                'data_source': 'dspace'
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
