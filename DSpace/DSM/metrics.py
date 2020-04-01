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
    USER_SENT_NUM = 'rgw_user_bytes_sent'  # 上传带宽
    USER_RECEIVED_NUM = 'rgw_user_bytes_received'  # 下载带宽
    USER_SENT_OPS = 'rgw_user_sent_ops'  # 上传请求
    USER_RECEIVED_OPS = 'rgw_user_received_ops'  # 下载请求
    USER_DELETE_OPS = 'rgw_user_delete_ops'  # 删除请求
    BUCKET_USED = 'rgw_bucket_kb_used'
    BUCKET_OBJ_NUM = 'rgw_bucket_object_num'
    BUCKET_SENT_NUM = 'rgw_bucket_bytes_sent'
    BUCKET_RECEIVED_NUM = 'rgw_bucket_bytes_received'
    BUCKET_SENT_OPS = 'rgw_bucket_sent_ops'
    BUCKET_RECEIVED_OPS = 'rgw_bucket_received_ops'
    BUCKET_DELETE_OPS = 'rgw_bucket_delete_ops'


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
        # TODO must rgw_obj inited
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

    def metrics_content(self, ctxt):
        logger.info('has sed metrics: %s', self.metrics.values())
        if not self.metrics:
            return ""
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        return ''.join(_metrics) + '\n'

    def collect_rgw_metrics_values(self):
        ctxt = get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        for cluster in clusters:
            ctxt.cluster_id = cluster.id
            active_rgws = objects.RadosgwList.get_all(
                ctxt, filters={'status': 'active'})
            if active_rgws:
                rgw = active_rgws[0]
                rgw_node = objects.Node.get_by_id(ctxt, rgw.node_id)
                service = str(rgw.ip_address) + ':' + str(rgw.port)
                self.check_agent_available(ctxt, rgw_node)
                client = self.agent_manager.get_client(rgw_node.id)
                obj_users = objects.ObjectUserList.get_all(ctxt)
                admin, access_key, secret_key = self.get_admin_user(ctxt)
                self.set_rgw_users_metrics_values(
                    ctxt, client, obj_users, access_key, secret_key, service)
                obj_buckets = objects.ObjectBucketList.get_all(ctxt)
                self.set_rgw_buckets_metrics_values(
                    ctxt, client, obj_buckets, access_key, secret_key, service)
            # TODO 每个agent上获取CPU等值
            rgw_nodes = self._get_rgw_node(ctxt)
            for rgw_node in rgw_nodes:
                pass
        logger.debug('collect_rgw_metrics:%s', self.metrics.values())

    def set_rgw_users_metrics_values(self, ctxt, agent_client, obj_users,
                                     access_key, secret_key, service):
        for obj_user in obj_users:
            uid = obj_user.uid
            self.set_user_capacity(ctxt, agent_client, access_key, secret_key,
                                   service, uid)
            self.set_user_bandwidth_and_ops(ctxt, agent_client, access_key,
                                            secret_key, service, uid)

    def set_user_capacity(self, ctxt, agent_client, access_key, secret_key,
                          service, uid):
        cluster_id = ctxt.cluster_id
        result = agent_client.get_rgw_user_capacity(
            ctxt, access_key, secret_key, service, uid)
        uid = result['uid']
        size_used = result['size_used']
        obj_num = result['obj_num']
        self.metrics[RgwMetricsKey.USER_USED].set(
            size_used, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_OBJ_NUM].set(
            obj_num, (cluster_id, uid))

    def set_user_bandwidth_and_ops(self, ctxt, agent_client, access_key,
                                   secret_key, service, uid):
        cluster_id = ctxt.cluster_id
        result = agent_client.get_rgw_user_usage(ctxt, access_key, secret_key,
                                                 service, uid)
        bytes_sent = result.get('bytes_sent')
        sent_ops = result.get('sent_ops')
        bytes_received = result.get('bytes_received')
        received_ops = result.get('received_ops')
        delete_ops = result.get('delete_ops')
        self.metrics[RgwMetricsKey.USER_SENT_NUM].set(
            bytes_sent, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_SENT_OPS].set(
            sent_ops, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_RECEIVED_NUM].set(
            bytes_received, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_RECEIVED_OPS].set(
            received_ops, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_DELETE_OPS].set(
            delete_ops, (cluster_id, uid))

    def set_rgw_buckets_metrics_values(self, ctxt, agent_client, obj_buckets,
                                       access_key, secret_key, service):
        self.set_buckets_capacity(ctxt, agent_client, obj_buckets, access_key,
                                  secret_key, service)
        self.set_buckets_bandwidth_and_ops(ctxt, agent_client, obj_buckets,
                                           access_key, secret_key, service)

    def set_buckets_capacity(self, ctxt, agent_client, obj_buckets, access_key,
                             secret_key, service):
        cluster_id = ctxt.cluster_id
        results = agent_client.get_all_rgw_buckets_capacity(
            ctxt, access_key, secret_key, service)
        for result in results:
            bucket = result['bucket']
            owner = result['owner']
            bucket_kb_used = result['bucket_kb_used']
            bucket_object_num = result['bucket_object_num']
            self.metrics[RgwMetricsKey.BUCKET_USED].set(
                bucket_kb_used, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_OBJ_NUM].set(
                bucket_object_num, (cluster_id, bucket, owner))

    def set_buckets_bandwidth_and_ops(self, ctxt, agent_client, obj_buckets,
                                      access_key, secret_key, service):
        cluster_id = ctxt.cluster_id
        results = agent_client.get_all_rgw_buckets_usage(ctxt, access_key,
                                                         secret_key, service)
        bu_names = [bucket.name for bucket in obj_buckets]
        for data in results:
            bucket = data['bucket']
            if bucket not in bu_names:
                continue
            owner = data['owner']
            bytes_sent = data['bytes_sent']
            sent_ops = data['sent_ops']
            bytes_received = data['bytes_received']
            received_ops = data['received_ops']
            delete_ops = data['delete_ops']
            self.metrics[RgwMetricsKey.BUCKET_SENT_NUM].set(
                bytes_sent, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_RECEIVED_NUM].set(
                bytes_received, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_SENT_OPS].set(
                sent_ops, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_RECEIVED_OPS].set(
                received_ops, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_DELETE_OPS].set(
                delete_ops, (cluster_id, bucket, owner))

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
        self.metrics[RgwMetricsKey.USER_SENT_NUM] = Metric(
            'count',
            RgwMetricsKey.USER_SENT_NUM,
            'Sent Num',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_RECEIVED_NUM] = Metric(
            'count',
            RgwMetricsKey.USER_RECEIVED_NUM,
            'Received Num',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_SENT_OPS] = Metric(
            'count',
            RgwMetricsKey.USER_SENT_OPS,
            'Sent Ops',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_RECEIVED_OPS] = Metric(
            'count',
            RgwMetricsKey.USER_RECEIVED_OPS,
            'Received Ops',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_DELETE_OPS] = Metric(
            'count',
            RgwMetricsKey.USER_DELETE_OPS,
            'Delete Ops',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.BUCKET_USED] = Metric(
            'gauge',
            RgwMetricsKey.BUCKET_USED,
            'Used Size',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_OBJ_NUM] = Metric(
            'gauge',
            RgwMetricsKey.BUCKET_OBJ_NUM,
            'Object Num Used',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_SENT_NUM] = Metric(
            'count',
            RgwMetricsKey.BUCKET_SENT_NUM,
            'Sent Num',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_RECEIVED_NUM] = Metric(
            'count',
            RgwMetricsKey.BUCKET_RECEIVED_NUM,
            'Received Num',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_SENT_OPS] = Metric(
            'count',
            RgwMetricsKey.BUCKET_SENT_OPS,
            'Sent Ops',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_RECEIVED_OPS] = Metric(
            'count',
            RgwMetricsKey.BUCKET_RECEIVED_OPS,
            'Received Ops',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_DELETE_OPS] = Metric(
            'count',
            RgwMetricsKey.BUCKET_DELETE_OPS,
            'Delete Ops',
            ('cluster_id', 'bucket', 'owner')
        )
