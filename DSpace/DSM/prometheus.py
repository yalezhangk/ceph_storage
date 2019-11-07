from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
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
        data = {}
        prometheus.osd_get_capacity(osd, data)
        prometheus.osd_disk_perf(osd)
        data.update(osd.metrics)
        return data

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
        metrics = {}
        prometheus.pool_get_capacity(pool, metrics)
        prometheus.pool_get_perf(pool, metrics)
        return metrics

    def pool_metrics_history_get(self, ctxt, pool_id, start, end):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.pool_get_histroy_capacity(pool, float(start), float(end),
                                             metrics)
        prometheus.pool_get_histroy_perf(pool, float(start), float(end),
                                         metrics)
        return metrics

    def disk_perf_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.disk_get_perf(disk)
        return data

    def disk_perf_history_get(self, ctxt, disk_id, start, end):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.disk_get_histroy_perf(disk,
                                                float(start),
                                                float(end))
        return data
