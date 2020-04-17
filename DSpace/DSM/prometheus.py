from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class PrometheusHandler(AdminBaseHandler):
    def cluster_metrics_get(self, ctxt):
        prometheus = PrometheusTool(ctxt)
        res = {}
        cluster_perf = {'cluster_read_bytes_sec', 'cluster_read_op_per_sec',
                        'cluster_recovering_bytes_per_sec',
                        'cluster_recovering_objects_per_sec',
                        'cluster_write_bytes_sec', 'cluster_write_op_per_sec',
                        'cluster_write_lat', 'cluster_read_lat',
                        'cluster_total_bytes', 'cluster_total_used_bytes'}

        for m in cluster_perf:
            metric = 'ceph_{}'.format(m)
            res[m] = prometheus.prometheus_get_metric(metric)
        prometheus.cluster_get_pg_state(res)
        return res

    def cluster_history_metrics_get(self, ctxt, start, end):
        prometheus = PrometheusTool(ctxt)
        res = {}
        cluster_perf = {'cluster_read_bytes_sec', 'cluster_read_op_per_sec',
                        'cluster_recovering_bytes_per_sec',
                        'cluster_recovering_objects_per_sec',
                        'cluster_write_bytes_sec', 'cluster_write_op_per_sec',
                        'cluster_write_lat', 'cluster_read_lat',
                        'cluster_total_bytes', 'cluster_total_used_bytes'}
        for m in cluster_perf:
            metric = 'ceph_{}'.format(m)
            res[m] = prometheus.prometheus_get_histroy_metric(
                metric, float(start), float(end))
        return res

    def node_metrics_monitor_get(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_monitor(node)
        return metrics

    def node_metrics_histroy_monitor_get(self, ctxt, node_id, start, end):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_histroy_monitor(
            node, float(start), float(end))
        return metrics

    def node_metrics_network_get(self, ctxt, node_id, net_name):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_network(node, net_name)
        return metrics

    def node_metrics_histroy_network_get(self, ctxt, node_id, net_name, start,
                                         end):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.node_get_metrics_histroy_network(
            node=node, net_name=net_name, start=float(start), end=float(end))
        return data

    def osd_metrics_get(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.osd_get_realtime_metrics(osd)
        return data

    def osd_metrics_history_get(self, ctxt, osd_id, start, end):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.osd_get_histroy_metrics(osd, float(start),
                                                  float(end))
        return data

    def osd_disk_metrics_get(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        prometheus.osd_get_capacity(osd)
        prometheus.osd_disk_perf(osd)
        return osd.metrics

    def osd_history_disk_metrics_get(self, ctxt, osd_id, start, end):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        metircs = {}
        prometheus.osd_get_histroy_capacity(
            osd, float(start), float(end), metircs)
        prometheus.osd_disk_histroy_metircs(
            osd, float(start), float(end), metircs)
        return metircs

    def pool_metrics_get(self, ctxt, pool_id):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        prometheus.pool_get_capacity(pool)
        prometheus.pool_get_perf(pool)
        return pool.metrics

    def pool_metrics_history_get(self, ctxt, pool_id, start, end):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.pool_get_histroy_capacity(pool, float(start), float(end),
                                             metrics)
        prometheus.pool_get_histroy_perf(pool, float(start), float(end),
                                         metrics)
        return metrics

    def pool_capacity_get(self, ctxt, pool_id):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        prometheus.pool_get_capacity(pool)
        prometheus.pool_get_pg_state(pool)
        return pool.metrics

    def disk_perf_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        prometheus = PrometheusTool(ctxt)
        prometheus.disk_get_perf(disk)
        return disk.metrics

    def disk_perf_history_get(self, ctxt, disk_id, start, end):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.disk_get_histroy_perf(disk,
                                                float(start),
                                                float(end))
        return data

    def object_user_metrics_get(self, ctxt, object_user_id):
        obj_user = objects.ObjectUser.get_by_id(ctxt, object_user_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.object_user_get_capacity(obj_user, metrics)
        prometheus.object_user_get_perf(obj_user, metrics)
        return metrics

    def object_user_metrics_history_get(self, ctxt, obj_user_id, start, end):
        obj_user = objects.ObjectUser.get_by_id(ctxt, obj_user_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.object_user_get_histroy_capacity(
            obj_user, float(start), float(end), metrics)
        prometheus.object_user_get_histroy_perf(
            obj_user, float(start), float(end), metrics)
        return metrics

    def object_bucket_metrics_get(self, ctxt, object_bucket_id):
        object_bucket = objects.ObjectBucket.get_by_id(ctxt, object_bucket_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.object_bucket_get_capacity(object_bucket, metrics)
        prometheus.object_bucket_get_perf(object_bucket, metrics)
        return metrics

    def object_bucket_metrics_history_get(self, ctxt, object_bucket_id, start,
                                          end):
        obj_bucket = objects.ObjectBucket.get_by_id(ctxt, object_bucket_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.object_bucket_get_histroy_capacity(
            obj_bucket, float(start), float(end), metrics)
        prometheus.object_bucket_get_histroy_perf(
            obj_bucket, float(start), float(end), metrics)
        return metrics

    def radosgw_metrics_get(self, ctxt, rgw_id):
        rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.radosgw_get_cpu_memory(rgw, metrics)
        prometheus.radosgw_get_perf(rgw, metrics)
        return metrics

    def radosgw_metrics_history_get(self, ctxt, rgw_id, start, end):
        rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.radosgw_get_histroy_cpu_memory(
            rgw, float(start), float(end), metrics)
        prometheus.radosgw_get_histroy_perf(
            rgw, float(start), float(end), metrics)
        return metrics

    def radosgw_router_metrics_get(self, ctxt, rgw_router_id, router_service):
        rgw_router = objects.RadosgwRouter.get_by_id(
            ctxt, rgw_router_id, expected_attrs=['router_services'])
        service_name = self.router_service_map(router_service)
        node_id = rgw_router.router_services[0].node_id
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.rgw_router_get_cpu_memory(
            rgw_router, metrics, service_name, node.hostname)
        return metrics

    def radosgw_router_metrics_history_get(self, ctxt, rgw_router_id, start,
                                           end, router_service):
        rgw_router = objects.RadosgwRouter.get_by_id(
            ctxt, rgw_router_id, expected_attrs=['router_services'])
        service_name = self.router_service_map(router_service)
        node_id = rgw_router.router_services[0].node_id
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.rgw_router_get_histroy_cpu_memory(
            rgw_router, float(start), float(end), metrics, service_name,
            node.hostname)
        return metrics

    def router_service_map(self, router_service):
        r_service_map = {'haproxy': 'athena_radosgw_haproxy',
                         'keepalived': 'athena_radosgw_keepalived'}
        name = r_service_map.get(router_service)
        if not name:
            raise InvalidInput(_('router_service name：%s not exist') %
                               router_service)
        return name

    def object_bucket_bandwidth_total(self, ctxt, bucket_id):
        object_bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.object_bucket_get_bandwidth_total(object_bucket, metrics)
        return metrics
