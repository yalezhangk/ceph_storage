from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType

logger = logging.getLogger(__name__)


class EmailGroupHandler(AdminBaseHandler):

    def email_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None, expected_attrs=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.EmailGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def email_group_get_count(self, ctxt, filters=None):
        return objects.EmailGroupList.get_count(ctxt, filters=filters)

    def email_group_create(self, ctxt, data):
        logger.debug('email_group:%s begin create', data.get('name'))
        email_group_data = {
            'name': data.get('name'),
            'emails': data.get('emails'),
            'cluster_id': ctxt.cluster_id
        }
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.CREATE)
        email_group = objects.EmailGroup(ctxt, **email_group_data)
        email_group.create()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           after_obj=email_group)
        logger.info('email_group:%s create success', email_group_data['name'])
        return email_group

    def email_group_get(self, ctxt, email_group_id, expected_attrs=None):
        return objects.EmailGroup.get_by_id(
            ctxt, email_group_id, expected_attrs)

    def email_group_update(self, ctxt, email_group_id, data):
        logger.debug('email_group:%s begin update', email_group_id)
        email_group = self.email_group_get(ctxt, email_group_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.UPDATE, before_obj=email_group)
        name = data.get('name')
        emails = data.get('emails')
        email_group.name = name
        email_group.emails = emails
        email_group.save()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           after_obj=email_group)
        logger.info('email_group:% update success', email_group_id)
        return email_group

    def email_group_delete(self, ctxt, email_group_id):
        logger.debug('email_group:%s begin delete', email_group_id)
        email_group = self.email_group_get(
            ctxt, email_group_id, expected_attrs=['alert_groups'])
        if email_group.alert_groups:
            raise exc.InvalidInput(reason=_(
                'can not delete email_group used by alert_group'))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.DELETE, before_obj=email_group)
        email_group.destroy()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           after_obj=email_group)
        logger.info('email_group:%s delete success', email_group_id)
        return email_group
