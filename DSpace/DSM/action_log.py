import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class ActionLogHandler(AdminBaseHandler):

    def action_log_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.ActionLogList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def action_log_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        action_log = objects.ActionLog(ctxt, **data)
        action_log.create()
        return action_log

    def action_log_get(self, ctxt, action_log_id):
        return objects.ActionLog.get_by_id(ctxt, action_log_id)

    def action_log_update(self, ctxt, action_log_id, data):
        action_log = self.action_log_get(ctxt, action_log_id)
        for k, v in six.iteritems(data):
            setattr(action_log, k, v)
        action_log.save()
        return action_log
