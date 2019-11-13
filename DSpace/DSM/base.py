from concurrent import futures

from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import objects
from DSpace.common.config import CONF

logger = logging.getLogger(__name__)


class AdminBaseHandler(object):
    def __init__(self):
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

    def begin_action(self, ctxt, resource_type=None, action=None):
        logger.debug('begin action:%s-%s', resource_type, action)
        action_data = {
            'begin_time': timeutils.utcnow(),
            'client_ip': ctxt.client_ip,
            'user_id': ctxt.user_id,
            'resource_type': resource_type,
            'action': action,
            'cluster_id': ctxt.cluster_id
        }
        action_log = objects.ActionLog(ctxt, **action_data)
        action_log.create()
        return action_log

    def finish_action(self, begin_action=None, resource_id=None,
                      resource_name=None, resource_data=None, status=None,
                      action=None):
        finish_data = {
            'resource_id': resource_id,
            'resource_name': resource_name,
            'resource_data': resource_data,
            'status': 'success',
            'finish_time': timeutils.utcnow()
        }
        if action:
            finish_data.update({'action': action})
        if status:
            if status in ['active', 'success']:
                finish_data.update({'status': 'success'})
            else:
                finish_data.update({'status': 'fail'})
        begin_action.update(finish_data)
        logger.debug('finish action:%s-%s,status:%s', resource_name, action,
                     finish_data['status'])
        begin_action.save()
