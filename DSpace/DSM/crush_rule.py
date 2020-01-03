from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class CrushRuleHandler(AdminBaseHandler):
    def crush_rule_create(self, ctxt, rule_name, failure_domain_type,
                          rule_content):
        crush_rule = objects.CrushRule(
            ctxt, rule_name=rule_name,
            type=failure_domain_type,
            content=rule_content)
        crush_rule.create()
        return crush_rule

    def crush_rule_get(self, ctxt, crush_rule_id):
        return objects.CrushRule.get_by_id(
            ctxt, crush_rule_id, expected_attrs=['osds'])

    def crush_rule_delete(self, ctxt, crush_rule_id):
        crush_rule = objects.CrushRule.get_by_id(ctxt, crush_rule_id)
        crush_rule.destroy()
        return crush_rule
