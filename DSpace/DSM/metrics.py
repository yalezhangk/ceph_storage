import time

from oslo_log import log as logging

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import get_context
from DSpace.DSM.base import AdminBaseHandler
from DSpace.utils.metrics import Metric

logger = logging.getLogger(__name__)


class RgwMetricsKey(object):
    USER_USED = 'rgw_user_kb_used'
    USER_OBJ_NUM = 'rgw_user_object_num'


class MetricsHandler(AdminBaseHandler):
    metrics_lock = None
    metrics = None
    """
    1. 设置监控项（各个KEY）set_key
    2. DSA 收集监控值 collect
    3. format 格式化成prometheus 需要的数据格式
    """

    def __init__(self, *args, **kwargs):
        super(MetricsHandler, self).__init__(*args, **kwargs)

    def bootstrap(self):
        super(MetricsHandler, self).bootstrap()
        self._setup_metrics()

    def _setup_metrics(self):
        self.metrics = {}
        self.rgw_metrics_init_keys()
        self.task_submit(self.collect_monitor_values)

    def collect_monitor_values(self):
        self.wait_ready()
        while True:
            try:
                self.collect_rgw_metrics_values()
            except Exception as e:
                logger.warning('collect_rgw_metrics_values Exception:%s', e)
            time.sleep(CONF.collect_metrics_time)

    def collect_rgw_metrics_values(self):
        ctxt = get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        for cluster in clusters:
            ctxt.cluster_id = cluster.id
            # get rgw nodes
            rgw_nodes = self._get_rgw_node(ctxt)
            active_rgws = objects.RadosgwList.get_all(
                ctxt, filters={'status': 'active'})
            if rgw_nodes and active_rgws:
                rgw = active_rgws[0]
                service = str(rgw.ip_address) + ':' + str(rgw.port)
                self.check_agent_available(ctxt, rgw_nodes[0])
                client = self.agent_manager.get_client(rgw_nodes[0].id)
                obj_users = objects.ObjectUserList.get_all(
                    ctxt, expected_attrs=['access_keys'])
                self._set_rgw_user_kb_used_and_obj_num(ctxt, client, obj_users,
                                                       service)
                # TODO collect other metrices value
            # TODO 每个agent上获取CPU等值
            for rgw_node in rgw_nodes:
                pass
        logger.debug('collect_rgw_metrics:%s', self.metrics.values())

    def metrics_content(self, ctxt):
        logger.info('has sed metrics: %s', self.metrics.values())
        if not self.metrics:
            return ""
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        return ''.join(_metrics) + '\n'

    def _set_rgw_user_kb_used_and_obj_num(self, ctxt, agent_client, obj_users,
                                          service):
        for obj_user in obj_users:
            obj_access = obj_user.access_keys[0]
            access_key = obj_access.access_key
            secret_key = obj_access.secret_key
            result = agent_client.get_rgw_user_kb_used_and_obj_num(
                ctxt, access_key, secret_key, service, obj_user.uid)
            cluster_id = result['cluster_id']
            uid = result['uid']
            size_used = result['size_used']
            obj_num = result['obj_num']
            # user size used
            self.metrics[RgwMetricsKey.USER_USED].set(
                size_used, (cluster_id, uid))
            # user obj num
            self.metrics[RgwMetricsKey.USER_OBJ_NUM].set(
                obj_num, (cluster_id, uid))

    def rgw_metrics_init_keys(self):
        self.metrics[RgwMetricsKey.USER_USED] = Metric(
            'gauge',
            RgwMetricsKey.USER_USED,
            'Used Size',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_OBJ_NUM] = Metric(
            'gauge',
            RgwMetricsKey.USER_OBJ_NUM,
            'Object Num Used',
            ('cluster_id', 'uid')
        )
