import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType

logger = logging.getLogger(__name__)


class EmailGroupHandler(AdminBaseHandler):

    def email_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.EmailGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def email_group_get_count(self, ctxt, filters=None):
        return objects.EmailGroupList.get_count(ctxt, filters=filters)

    def email_group_create(self, ctxt, data):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.CREATE)
        data.update({'cluster_id': ctxt.cluster_id})
        email_group = objects.EmailGroup(ctxt, **data)
        email_group.create()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           resource_data=objects.json_encode(email_group))
        return email_group

    def email_group_get(self, ctxt, email_group_id):
        return objects.EmailGroup.get_by_id(ctxt, email_group_id)

    def email_group_update(self, ctxt, email_group_id, data):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.UPDATE)
        email_group = self.email_group_get(ctxt, email_group_id)
        for k, v in six.iteritems(data):
            setattr(email_group, k, v)
        email_group.save()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           resource_data=objects.json_encode(email_group))
        return email_group

    def email_group_delete(self, ctxt, email_group_id):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.EMAIL_GROUP,
            action=AllActionType.DELETE)
        email_group = self.email_group_get(ctxt, email_group_id)
        email_group.destroy()
        self.finish_action(begin_action, resource_id=email_group.id,
                           resource_name=email_group.name,
                           resource_data=objects.json_encode(email_group))
        return email_group
