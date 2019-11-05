import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

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
        data.update({'cluster_id': ctxt.cluster_id})
        emai_group = objects.EmailGroup(ctxt, **data)
        emai_group.create()
        return emai_group

    def email_group_get(self, ctxt, email_group_id):
        return objects.EmailGroup.get_by_id(ctxt, email_group_id)

    def email_group_update(self, ctxt, email_group_id, data):
        email_group = self.email_group_get(ctxt, email_group_id)
        for k, v in six.iteritems(data):
            setattr(email_group, k, v)
        email_group.save()
        return email_group

    def email_group_delete(self, ctxt, email_group_id):
        email_group = self.email_group_get(ctxt, email_group_id)
        email_group.destroy()
        return email_group
