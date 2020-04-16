import time

from oslo_log import log as logging

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import get_context
from DSpace.DSM.base import AdminBaseHandler
from DSpace.exception import RPCConnectError
from DSpace.objects.fields import RouterServiceStatus
from DSpace.utils.metrics import Metric

logger = logging.getLogger(__name__)


class RgwMetricsKey(object):
    USER_USED = 'rgw_user_kb_used'
    USER_TOTAL = 'rgw_user_kb_total'
    USER_OBJ_NUM = 'rgw_user_object_num'
    USER_SENT_NUM = 'rgw_user_sent_bytes_total'  # 上传带宽
    USER_RECEIVED_NUM = 'rgw_user_received_bytes_total'  # 下载带宽
    USER_SENT_OPS = 'rgw_user_sent_ops_total'  # 上传请求
    USER_RECEIVED_OPS = 'rgw_user_received_ops_total'  # 下载请求
    USER_DELETE_OPS = 'rgw_user_delete_ops_total'  # 删除请求
    BUCKET_USED = 'rgw_bucket_kb_used'
    BUCKET_TOTAL = 'rgw_bucket_kb_total'
    BUCKET_OBJ_NUM = 'rgw_bucket_object_num'
    BUCKET_SENT_NUM = 'rgw_bucket_sent_bytes_total'
    BUCKET_RECEIVED_NUM = 'rgw_bucket_received_bytes_total'
    BUCKET_SENT_OPS = 'rgw_bucket_sent_ops_total'
    BUCKET_RECEIVED_OPS = 'rgw_bucket_received_ops_total'
    BUCKET_DELETE_OPS = 'rgw_bucket_delete_ops_total'
    GATEWAY_CPU = 'rgw_gateway_cpu_rate_percent'
    GATEWAY_MEMORY = 'rgw_gateway_memory_rate_percent'
    ROUTER_CPU = 'rgw_router_cpu_rate_percent'
    ROUTER_MEMORY = 'rgw_router_memory_rate_percent'
    SYS_MEMORY = 'sys_total_memory_kb'


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
            except RPCConnectError as e:
                logger.warning('collect_rgw_metrics_values Warning: %s', e)
                time.sleep(2)
                continue
            except Exception as e:
                logger.exception('collect_rgw_metrics_values Exception:%s', e)
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
            # 1. set rgw user、bucket metrics
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
            # 2. set rgw_gateway metrics
            rgw_nodes = self._get_rgw_node(ctxt)
            for rgw_node in rgw_nodes:
                self.check_agent_available(ctxt, rgw_node)
                client = self.agent_manager.get_client(rgw_node.id)
                rgw_gateways = objects.RadosgwList.get_all(
                    ctxt, filters={'node_id': rgw_node.id})
                self.set_rgw_gateway_metrics_values(ctxt, client, rgw_gateways)
            # 3. set rgw_router metrics
            router_services = objects.RouterServiceList.get_all(
                ctxt, filters={'status': RouterServiceStatus.ACTIVE})
            node_ids = set([r_ser.node_id for r_ser in router_services])
            for node_id in node_ids:
                router_node = objects.Node.get_by_id(ctxt, node_id)
                self.check_agent_available(ctxt, router_node)
                client = self.agent_manager.get_client(node_id)
                self.set_rgw_router_metrics_values(ctxt, client, router_node)

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
        kb_size_total = result['kb_size_total']
        self.metrics[RgwMetricsKey.USER_USED].set(
            size_used, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_OBJ_NUM].set(
            obj_num, (cluster_id, uid))
        self.metrics[RgwMetricsKey.USER_TOTAL].set(
            kb_size_total, (cluster_id, uid))

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
            bucket_kb_total = result['bucket_kb_total']
            self.metrics[RgwMetricsKey.BUCKET_USED].set(
                bucket_kb_used, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_OBJ_NUM].set(
                bucket_object_num, (cluster_id, bucket, owner))
            self.metrics[RgwMetricsKey.BUCKET_TOTAL].set(
                bucket_kb_total, (cluster_id, bucket, owner))

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

    def set_rgw_gateway_metrics_values(self, ctxt, agent_client, rgw_gateways):
        cluster_id = ctxt.cluster_id
        names = [rgw.name for rgw in rgw_gateways]
        results = agent_client.get_rgw_gateway_cup_memory(ctxt, names)
        for data in results:
            ceph_daemon = data['ceph_daemon']
            cpu_percent = data['cpu_percent']
            memory_percent = data['memory_percent']
            self.metrics[RgwMetricsKey.GATEWAY_CPU].set(
                cpu_percent, (cluster_id, ceph_daemon))
            self.metrics[RgwMetricsKey.GATEWAY_MEMORY].set(
                memory_percent, (cluster_id, ceph_daemon))

    def set_rgw_router_metrics_values(self, ctxt, agent_client, router_node):
        cluster_id = ctxt.cluster_id
        hostname = router_node.hostname
        result_keep, result_ha = agent_client.get_rgw_router_cup_memory(ctxt)
        keep_service_name = result_keep['container_name']
        keep_cpu_percent = float(result_keep['cpu_usage_rate_percent'])
        keep_memory_percent = float(result_keep['memory_rate_percent'])
        ha_service_name = result_ha['container_name']
        ha_cpu_percent = float(result_ha['cpu_usage_rate_percent'])
        ha_memory_percent = float(result_ha['memory_rate_percent'])
        sys_memory_kb = result_ha['sys_memory_kb']
        # set values
        self.metrics[RgwMetricsKey.ROUTER_CPU].set(
            keep_cpu_percent, (cluster_id, hostname, keep_service_name))
        self.metrics[RgwMetricsKey.ROUTER_MEMORY].set(
            keep_memory_percent, (cluster_id, hostname, keep_service_name))
        self.metrics[RgwMetricsKey.ROUTER_CPU].set(
            ha_cpu_percent, (cluster_id, hostname, ha_service_name))
        self.metrics[RgwMetricsKey.ROUTER_MEMORY].set(
            ha_memory_percent, (cluster_id, hostname, ha_service_name))
        self.metrics[RgwMetricsKey.SYS_MEMORY].set(
            sys_memory_kb, (cluster_id, hostname))

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
            'counter',
            RgwMetricsKey.USER_SENT_NUM,
            'Sent Num',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_RECEIVED_NUM] = Metric(
            'counter',
            RgwMetricsKey.USER_RECEIVED_NUM,
            'Received Num',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_SENT_OPS] = Metric(
            'counter',
            RgwMetricsKey.USER_SENT_OPS,
            'Sent Ops',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_RECEIVED_OPS] = Metric(
            'counter',
            RgwMetricsKey.USER_RECEIVED_OPS,
            'Received Ops',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.USER_DELETE_OPS] = Metric(
            'counter',
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
            'counter',
            RgwMetricsKey.BUCKET_SENT_NUM,
            'Sent Num',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_RECEIVED_NUM] = Metric(
            'counter',
            RgwMetricsKey.BUCKET_RECEIVED_NUM,
            'Received Num',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_SENT_OPS] = Metric(
            'counter',
            RgwMetricsKey.BUCKET_SENT_OPS,
            'Sent Ops',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_RECEIVED_OPS] = Metric(
            'counter',
            RgwMetricsKey.BUCKET_RECEIVED_OPS,
            'Received Ops',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.BUCKET_DELETE_OPS] = Metric(
            'counter',
            RgwMetricsKey.BUCKET_DELETE_OPS,
            'Delete Ops',
            ('cluster_id', 'bucket', 'owner')
        )
        self.metrics[RgwMetricsKey.GATEWAY_CPU] = Metric(
            'gauge',
            RgwMetricsKey.GATEWAY_CPU,
            'CPU Rate %',
            ('cluster_id', 'ceph_daemon')
        )
        self.metrics[RgwMetricsKey.GATEWAY_MEMORY] = Metric(
            'gauge',
            RgwMetricsKey.GATEWAY_MEMORY,
            'Memory Rate %',
            ('cluster_id', 'ceph_daemon')
        )
        self.metrics[RgwMetricsKey.ROUTER_CPU] = Metric(
            'gauge',
            RgwMetricsKey.ROUTER_CPU,
            'CPU Rate %',
            ('cluster_id', 'hostname', 'service_name')
        )
        self.metrics[RgwMetricsKey.ROUTER_MEMORY] = Metric(
            'gauge',
            RgwMetricsKey.ROUTER_MEMORY,
            'Memory Rate %',
            ('cluster_id', 'hostname', 'service_name')
        )
        self.metrics[RgwMetricsKey.SYS_MEMORY] = Metric(
            'gauge',
            RgwMetricsKey.SYS_MEMORY,
            'Total Memory KB',
            ('cluster_id', 'hostname')
        )
        self.metrics[RgwMetricsKey.USER_TOTAL] = Metric(
            'counter',
            RgwMetricsKey.USER_TOTAL,
            'Total Size KB, -1 is Unlimited',
            ('cluster_id', 'uid')
        )
        self.metrics[RgwMetricsKey.BUCKET_TOTAL] = Metric(
            'counter',
            RgwMetricsKey.BUCKET_TOTAL,
            'Total Size KB, -1 is Unlimited',
            ('cluster_id', 'bucket', 'owner')
        )
