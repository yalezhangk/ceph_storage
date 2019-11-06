from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType

logger = logging.getLogger(__name__)


class AlertGroupHandler(AdminBaseHandler):
    def alert_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None, expected_attrs=None):
        return objects.AlertGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def alert_group_create(self, ctxt, data):
        ale_group_data = {
            'name': data.get('name'),
            'alert_rule_ids': data.get('alert_rule_ids'),
            'email_group_ids': data.get('email_group_ids')
        }
        begin_action = self.begin_action(
            ctxt, AllResourceType.ALERT_GROUP, AllActionType.CREATE)
        alert_group = objects.AlertGroup(ctxt, **ale_group_data)
        alert_group.create()
        self.finish_action(begin_action, alert_group.id, alert_group.name,
                           objects.json_encode(alert_group))
        logger.info('alert_group:%s create success', ale_group_data['name'])
        return alert_group

    def alert_group_get(self, ctxt, alert_group_id, expected_attrs=None):
        return objects.AlertGroup.get_by_id(ctxt, alert_group_id,
                                            expected_attrs)

    def alert_group_update(self, ctxt, alert_group_id, data):
        alert_group = self.alert_group_get(ctxt, alert_group_id)
        begin_action = self.begin_action(
            ctxt, AllResourceType.ALERT_GROUP, AllActionType.UPDATE)
        name = data.get('name')
        alert_rule_ids = data.get('alert_rule_ids')
        email_group_ids = data.get('email_group_ids')
        if name:
            alert_group.name = name
            alert_group.save()
            self.finish_action(begin_action, alert_group.id, alert_group.name,
                               objects.json_encode(alert_group))
            logger.info('alert_group:%s modify_name success', name)
        if alert_rule_ids:
            alert_group.alert_rule_ids = alert_rule_ids
            alert_group.save()
            self.finish_action(begin_action, alert_group.id, alert_group.name,
                               objects.json_encode(alert_group),
                               action=AllActionType.MODIFY_ALERT_RULES)
            logger.info('alert_group:%s modify_alert_rules success',
                        alert_group.name)
        if email_group_ids:
            alert_group.email_group_ids = email_group_ids
            alert_group.save()
            self.finish_action(begin_action, alert_group.id, alert_group.name,
                               objects.json_encode(alert_group),
                               action=AllActionType.MODIFY_EMAIL_GROUPS)
            logger.info('alert_group:%s modify_email_groups success',
                        alert_group.name)
        return alert_group

    def alert_group_delete(self, ctxt, alert_group_id):
        alert_group = self.alert_group_get(ctxt, alert_group_id)
        begin_action = self.begin_action(
            ctxt, AllResourceType.ALERT_GROUP, AllActionType.DELETE)
        alert_group.destroy()
        self.finish_action(begin_action, alert_group.id, alert_group.name,
                           objects.json_encode(alert_group))
        logger.info('alert_group:%s delete success',
                    alert_group.name)
        return alert_group

    def alert_group_get_count(self, ctxt, filters=None):
        return objects.AlertGroupList.get_count(
            ctxt, filters=filters)
