from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects.fields import AllActionStatus
from DSpace.objects.fields import AllResourceType
from DSpace.objects.fields import ResourceAction

logger = logging.getLogger(__name__)


class ActionLogHandler(AdminBaseHandler):

    def action_log_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        return objects.ActionLogList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def action_log_get(self, ctxt, action_log_id):
        return objects.ActionLog.get_by_id(ctxt, action_log_id)

    def action_log_get_count(self, ctxt, filters=None):
        return objects.ActionLogList.get_count(
            ctxt, filters=filters)

    def resource_action(self, ctxt):
        resource_type = AllResourceType.ALL
        action_status = AllActionStatus.ALL
        resource_action = ResourceAction.relation_resource_action()
        return {
            'resource_type': resource_type,
            'action_status': action_status,
            'resource_action': resource_action
        }
