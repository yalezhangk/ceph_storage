import sys
from concurrent import futures

from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import context
from DSpace import exception as exc
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSA.client import AgentClientManager
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import ConfigKey
from DSpace.utils.service_map import ServiceMap

logger = logging.getLogger(__name__)


class AdminBaseHandler(object):
    slow_requests = {}

    def __init__(self):
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)
        ctxt = context.get_context()
        self.debug_mode = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DEBUG_MODE)
        self.container_namespace = objects.sysconfig.sys_config_get(
            ctxt, "image_namespace")
        self.map_util = ServiceMap(self.container_namespace)

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

    def begin_action(self, ctxt, resource_type=None, action=None,
                     before_obj=None):
        logger.debug('begin action, resource_type:%s, action:%s',
                     resource_type, action)
        action_data = {
            'begin_time': timeutils.utcnow(),
            'client_ip': ctxt.client_ip,
            'user_id': ctxt.user_id,
            'resource_type': resource_type,
            'action': action,
            'before_data': (objects.json_encode(before_obj)
                            if before_obj else None),
            'cluster_id': ctxt.cluster_id
        }
        action_log = objects.ActionLog(ctxt, **action_data)
        action_log.create()
        return action_log

    def finish_action(self, begin_action=None, resource_id=None,
                      resource_name=None, after_obj=None, status=None,
                      action=None, err_msg=None, diff_data=None,
                      *args, **kwargs):
        finish_data = {
            'resource_id': resource_id,
            'resource_name': resource_name,
            'after_data': (objects.json_encode(after_obj) if
                           after_obj else None),
            'status': 'success',
            'finish_time': timeutils.utcnow(),
            'err_msg': err_msg,
            'diff_data': diff_data
        }
        if action:
            finish_data.update({'action': action})
        if status:
            if status in ['active', 'success', 'available']:
                finish_data.update({'status': 'success'})
            else:
                if self.debug_mode:
                    sys.exit(1)
                finish_data.update({'status': 'fail'})
        begin_action.update(finish_data)
        logger.debug('finish action, resource_name:%s, action:%s, status:%s',
                     resource_name, action, finish_data['status'])
        begin_action.save()

    def has_monitor_host(self, ctxt):
        filters = {'status': s_fields.NodeStatus.ACTIVE,
                   'role_monitor': True}
        mon_host = objects.NodeList.get_all(ctxt, filters=filters)
        cluster_id = ctxt.cluster_id
        if not mon_host:
            logger.info('has not active mon host, cluster_id={}'.format(
                cluster_id))
            return False
        return True

    def check_mon_host(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if not has_mon_host:
            raise exc.InvalidInput(
                reason=_('has not active mon host, can not do any '
                         'ceph actions'))

    def check_service_status(self, ctxt, service):
        time_now = timeutils.utcnow(with_timezone=True)
        if service.updated_at:
            update_time = service.updated_at
        else:
            update_time = service.created_at
        time_diff = time_now - update_time
        if (service.status == s_fields.ServiceStatus.ACTIVE) \
                and (time_diff.total_seconds() > CONF.service_max_interval):
            service.status = s_fields.ServiceStatus.INACTIVE
            service.save()

    def notify_node_update(self, ctxt, node):
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node.id)
        client.node_update_infos(ctxt, node)
