import six
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

    def alert_rule_get(self, ctxt, alert_rule_id):
        return objects.AlertRule.get_by_id(ctxt, alert_rule_id)

    def alert_rule_update(self, ctxt, alert_rule_id, data):
        rule = self.alert_rule_get(ctxt, alert_rule_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ALERT_RULE,
            action=AllActionType.OPEN_OR_CLOSE_RULE)
        rule_data = {
            'enabled': data.get('enabled')
        }
        for k, v in six.iteritems(rule_data):
            setattr(rule, k, v)
        rule.save()
        self.finish_action(begin_action, resource_id=rule.id,
                           resource_name=rule.type,
                           resource_data=objects.json_encode(rule))
        return rule

    def alert_rule_get_count(self, ctxt, filters=None):
        return objects.AlertRuleList.get_count(
            ctxt, filters=filters)
