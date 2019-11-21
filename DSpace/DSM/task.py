from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class TaskHandler(AdminBaseHandler):
    def task_get_all(self, ctxt, tab=None, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        tasks = objects.TaskList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return tasks

    def task_get_count(self, ctxt, filters=None):
        count = objects.TaskList.get_count(ctxt, filters=filters)
        return count
