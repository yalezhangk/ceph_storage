import json
import logging

import six
from prometheus_http_client import NodeExporter
from prometheus_http_client import Prometheus as PrometheusClient
from prometheus_http_client.prometheus import relabel

from DSpace import exception
from DSpace import objects
from DSpace.DSM.metrics import RgwMetricsKey as MeK


class DSpaceNodeExporter(NodeExporter):
    @relabel('node_filesystem_size_bytes{}')
    def node_sys_disk_size(self, **kwargs):
        pass

    @relabel('(node_filesystem_size_bytes{} - node_filesystem_free_bytes{}) '
             ' / (node_filesystem_size_bytes{} - node_filesystem_free_bytes{} '
             '+ node_filesystem_avail_bytes{})')
    def node_sys_disk_used_rate(self, **kwargs):
        pass


DISK_SKIP = "dm.*"

logger = logging.getLogger(__name__)

cpu_attrs = ['cpu_rate', 'intr_rate', 'context_switches_rate',
             'vmstat_pgfault_rate', 'load5']
sys_attrs = ['memory_rate']

sys_disk_attrs = ['sys_disk_size', 'sys_disk_used_rate']

disk_attrs = ['disk_io_rate']

network_attrs = ['network_transmit_packets_rate',
                 'network_receive_packets_rate',
                 'network_transmit_rate', 'network_receive_rate',
                 'network_errs_rate', 'network_drop_rate']

prometheus_attrs = cpu_attrs + sys_attrs + disk_attrs + network_attrs +\
    sys_disk_attrs

osd_rate = ['op_in_bytes', 'op_out_bytes', 'op_w',
            'op_r', 'op_w_latency', 'op_r_latency']

osd_capacity = ['kb', 'kb_avail', 'kb_used']

pool_capacity = ['max_avail', 'bytes_used', 'raw_bytes_used']

pool_perf = ['read_bytes_sec', 'write_op_per_sec', 'write_bytes_sec',
             'read_op_per_sec', 'recovering_objects_per_sec',
             'recovering_bytes_per_sec', 'write_lat', 'read_lat']

pool_total_perf_map = {
    'total_bytes': 'ceph_pool_read_bytes_sec + ceph_pool_write_bytes_sec',
    'total_ops': 'ceph_pool_read_op_per_sec + ceph_pool_write_op_per_sec'
}

bluefs_capacity = ['db_total_bytes', 'db_used_bytes',
                   'wal_total_bytes', 'wal_used_bytes',
                   'slow_total_bytes', 'slow_used_bytes']

disk_metrics = ['write_iops_rate', 'read_iops_rate', 'write_bytes_rate',
                'read_bytes_rate', 'write_lat_rate', 'read_lat_rate',
                'io_rate']

node_default_attrs = ['cpu_rate', 'memory_rate']

node_cpu_attrs = ['cpu_rate', 'memory_rate', 'intr_rate',
                  'context_switches_rate', 'load5', 'vmstat_pgfault_rate']

node_network_attrs = ['network_transmit_packets_rate',
                      'network_receive_packets_rate',
                      'network_transmit_rate', 'network_receive_rate',
                      'network_errs_rate', 'network_drop_rate']

object_user_capacity_attrs = [MeK.USER_USED, MeK.USER_OBJ_NUM]

object_user_perf_attrs = [MeK.USER_SENT_NUM, MeK.USER_RECEIVED_NUM,
                          MeK.USER_SENT_OPS, MeK.USER_RECEIVED_OPS,
                          MeK.USER_DELETE_OPS]

object_bucket_capacity_attrs = [MeK.BUCKET_USED, MeK.BUCKET_OBJ_NUM]

object_bucket_perf_attrs = [MeK.BUCKET_SENT_NUM, MeK.BUCKET_RECEIVED_NUM,
                            MeK.BUCKET_SENT_OPS, MeK.BUCKET_RECEIVED_OPS,
                            MeK.BUCKET_DELETE_OPS]

rgw_gateway_cpu_memory_attrs = [MeK.GATEWAY_CPU, MeK.GATEWAY_MEMORY]

rgw_gateway_perf_attrs = ['rgw_put', 'rgw_get', 'rgw_put_b', 'rgw_get_b']

rgw_router_cpu_memory_attrs = [MeK.ROUTER_CPU, MeK.ROUTER_MEMORY]


class PrometheusTool(object):
    prometheus_url = None

    def __init__(self, ctxt):
        self.ctxt = ctxt
        self._get_prometheus_endpoint()

    def _get_prometheus_endpoint(self):
        if not self.prometheus_url:
            endpoints = objects.RPCServiceList.get_all(
                self.ctxt,
                filters={
                    "service_name": 'prometheus'
                }
            )
            if not endpoints:
                raise exception.PrometheusEndpointNotFound(
                    service_name='prometheus', cluster_id=self.ctxt.cluster_id)
            endpoint = endpoints[0].endpoint
            self.prometheus_url = "http://{}:{}".format(endpoint['ip'],
                                                        endpoint['port'])

    def _get_sys_disk(self, node_id):
        disks = objects.DiskList.get_all(
            self.ctxt, filters={"node_id": node_id, "role": "system",
                                "cluster_id": self.ctxt.cluster_id})
        if len(disks) == 0:
            logger.warning("prometheus: cannot find sys disk: %s", node_id)
            return None
        disk_name = disks[0].name
        logger.debug("prometheus: get sys disk name for node %s: %s",
                     node_id, disk_name)
        return disk_name

    def _get_net_name(self, node_id, ipaddr):
        network = objects.NetworkList.get_all(self.ctxt, filters={
            'node_id': node_id,
            'ip_address': str(ipaddr),
            'cluster_id': self.ctxt.cluster_id,
        })
        if len(network) == 0:
            logger.warning("prometheus: cannot find network: node %s ip %s",
                           node_id, ipaddr)
            return None
        net_name = network[0].name
        logger.debug("prometheus: get network name for node %s and ip %s: %s",
                     node_id, ipaddr, net_name)
        return net_name

    def prometheus_get_metric(self, metric, filter=None, not_filter=None):
        """Get metrics from prometheus
        when not_filter is True, filter=None
        Returns: metrics value
        """
        filter = filter or {}
        if "cluster_id" not in filter.keys():
            filter['cluster_id'] = self.ctxt.cluster_id
        if not_filter is True:
            filter = None
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query(metric=metric, filter=filter))
        except Exception as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            return None

        if len(value['data']['result']):
            data = value['data']['result'][0]['value']
            logger.info('get metric:%s data success from prometheus, data:%s',
                        metric, data)
            return data
        else:
            logger.info('get metric:%s data is None from prometheus', metric)
            return None

    def prometheus_get_metrics(self, metric, filter=None):
        """Get metrics from prometheus
        Returns: metrics value
        """
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query(metric=metric, filter=filter))
        except Exception as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            return None

        if len(value['data']['result']):
            data = value['data']['result']
            logger.info('get metric:%s data success from prometheus, data:%s',
                        metric, data)
            return data
        else:
            logger.info('get metric:%s data is None from prometheus', metric)
            return None

    def prometheus_get_histroy_metric(self, metric, start, end, filter=None,
                                      not_filter=None):
        """Get metrics from prometheus
        when not_filter is True, filter=None
        Returns: metrics value
        """
        filter = filter or {}
        if "cluster_id" not in filter.keys():
            filter['cluster_id'] = self.ctxt.cluster_id
        if not_filter is True:
            filter = None
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query_rang(
                metric=metric, filter=filter, start=start, end=end))
        except Exception:
            return None

        if len(value['data']['result']):
            return value['data']['result'][0]['values']
        else:
            return None

    def get_node_exporter_metric(self, metric, **kwargs):
        """Get metrics from prometheus
        Returns: metrics value
        """
        node_exporter = DSpaceNodeExporter(url=self.prometheus_url)

        function = getattr(node_exporter, metric)

        graph = kwargs.get('graph')
        try:
            value = json.loads(function(**kwargs))
        except Exception:
            return None

        if len(value['data']['result']):
            if graph:
                return value['data']['result'][0]['values']
            else:
                return value['data']['result'][0]['value']
        else:
            return None

    def get_node_exporter_metrics(self, metric, **kwargs):
        """Get metrics from prometheus
        Returns: metrics value
        """
        node_exporter = DSpaceNodeExporter(url=self.prometheus_url)

        function = getattr(node_exporter, metric)

        graph = kwargs.get('graph')
        try:
            value = json.loads(function(**kwargs))
        except Exception:
            return None

        if len(value['data']['result']):
            if graph:
                return value['data']['result']
            else:
                return value['data']['result']
        else:
            return None

    def node_get_metrics_network(self, node, net_name):
        metrics = {}
        for metric in network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': net_name,
                                       'cluster_id': node.cluster_id})
            metrics.update({metric: data})
        return metrics

    def node_get_metrics_monitor(self, node):
        sys_disk_name = self._get_sys_disk(node.id)
        metrics = {
            "cpu_rate": self.get_node_exporter_metric(
                "node_cpu_rate", filter={'hostname': node.hostname,
                                         'cluster_id': node.cluster_id}),
            "memory_rate": self.get_node_exporter_metric(
                "node_memory_rate", filter={'hostname': node.hostname,
                                            'cluster_id': node.cluster_id}),
            "load5": self.get_node_exporter_metric("node_load5", filter={
                'hostname': node.hostname, 'cluster_id': node.cluster_id}),
            "disk_io_rate": self.get_node_exporter_metric(
                "node_disk_io_rate", filter={'hostname': node.hostname,
                                             'device': sys_disk_name,
                                             'cluster_id': node.cluster_id})
        }
        net_name = self._get_net_name(node.id, node.ip_address)
        for m in network_attrs:
            metric_method = 'node_{}'.format(m)
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': net_name,
                                       'cluster_id': node.cluster_id})
            metrics.update({m: data})
        return metrics

    def node_get_metrics_histroy_monitor(self, node, start, end):
        sys_disk_name = self._get_sys_disk(node.id)
        metrics = {
            "cpu_rate": self.get_node_exporter_metric(
                "node_cpu_rate", filter={'hostname': node.hostname,
                                         'cluster_id': node.cluster_id},
                graph=True, start=start, end=end),
            "memory_rate": self.get_node_exporter_metric(
                "node_memory_rate", filter={'hostname': node.hostname,
                                            'cluster_id': node.cluster_id},
                graph=True, start=start, end=end),
            "load5": self.get_node_exporter_metric(
                "node_load5", filter={'hostname': node.hostname,
                                      'cluster_id': node.cluster_id},
                graph=True, start=start, end=end),
            "disk_io_rate": self.get_node_exporter_metric(
                "node_disk_io_rate",
                filter={'hostname': node.hostname,
                        'device': sys_disk_name,
                        'cluster_id': node.cluster_id},
                graph=True, start=start, end=end)
        }
        net_name = self._get_net_name(node.id, node.ip_address)
        for m in network_attrs:
            metric_method = 'node_{}'.format(m)
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname,
                        'device': net_name,
                        'cluster_id': node.cluster_id},
                graph=True, start=start, end=end)
            metrics.update({m: data})
        return metrics

    def node_get_metrics_histroy_network(self, node, net_name, start, end):
        metrics = {}
        for metric in network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname,
                        'device': net_name,
                        'cluster_id': node.cluster_id},
                graph=True, start=start, end=end)
            metrics.update({metric: data})
        return metrics

    def node_get_metrics_overall(self, node):
        net_name = self._get_net_name(node.id, node.ip_address)
        sys_disk_name = self._get_sys_disk(node.id)
        if not net_name or not sys_disk_name:
            return
        for metric in prometheus_attrs:
            metric_method = 'node_{}'.format(metric)
            if metric in network_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'device': net_name,
                        'cluster_id': node.cluster_id})
            elif metric in disk_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'device': sys_disk_name,
                        'cluster_id': node.cluster_id})
            elif metric in sys_disk_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'mountpoint': '/',
                        'cluster_id': node.cluster_id})
            else:
                data = self.get_node_exporter_metric(metric_method, filter={
                    'hostname': node.hostname, 'cluster_id': node.cluster_id})
            if not data:
                node.metrics.clear()
                return
            node.metrics.update({metric: data})

    def nodes_get_default_metrics(self, nodes):
        logger.info("osds get capacity")
        nodes_default = {}
        for node in nodes:
            nodes_default[node.hostname] = node
        for m in node_default_attrs:
            method = 'node_{}'.format(m)
            datas = self.get_node_exporter_metrics(
                method, filter={'cluster_id': self.ctxt.cluster_id})
            if not datas:
                continue
            for data in datas:
                hostname = data['metric']['hostname']
                value = data['value']
                if hostname in nodes_default.keys():
                    node = nodes_default[hostname]
                    node.metrics.update({m: value})

    def nodes_get_cpu_metrics(self, nodes):
        logger.info("osds get capacity")
        nodes_cpu = {}
        for node in nodes:
            nodes_cpu[node.hostname] = node
        for m in node_cpu_attrs:
            method = 'node_{}'.format(m)
            datas = self.get_node_exporter_metrics(
                method, filter={'cluster_id': self.ctxt.cluster_id})
            if not datas:
                continue
            for data in datas:
                hostname = data['metric']['hostname']
                value = data['value']
                if hostname in nodes_cpu.keys():
                    node = nodes_cpu[hostname]
                    node.metrics.update({m: value})

    def nodes_get_network_metrics(self, nodes):
        logger.info("osds get capacity")
        nodes_network = {}
        for node in nodes:
            net_name = self._get_net_name(node.id, node.ip_address)
            nodes_network[node.hostname] = {
                'node': node,
                'net_name': net_name
            }
        for m in node_network_attrs:
            method = 'node_{}'.format(m)
            datas = self.get_node_exporter_metrics(
                method, filter={'cluster_id': self.ctxt.cluster_id})
            if not datas:
                continue
            for data in datas:
                hostname = data['metric']['hostname']
                net_name = data['metric']['device']
                value = data['value']
                if (hostname in nodes_network.keys() and
                        net_name == nodes_network[hostname]['net_name']):
                    node = nodes_network[hostname]['node']
                    node.metrics.update({m: value})

    def disk_get_perf(self, disk):
        node = objects.Node.get_by_id(self.ctxt, disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': disk.name,
                                       'cluster_id': disk.cluster_id})
            disk.metrics.update({m: data})

    def disk_get_histroy_perf(self, disk, start, end):
        node = objects.Node.get_by_id(self.ctxt, disk.node_id)
        metrics = {}
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname,
                        'device': disk.name,
                        'cluster_id': disk.cluster_id},
                graph=True, start=start, end=end)
            metrics.update({m: data})
        return metrics

    def cluster_get_pg_state(self):
        prometheus = PrometheusClient(url=self.prometheus_url)
        cluster_pg_state = None
        try:
            pg_value = json.loads(
                prometheus.query(metric='ceph_pg_metadata')
            )['data']['result']
            healthy = 0
            degraded = 0
            recovering = 0
            unactive = 0
            pg_total = 0.0
            for pg in pg_value:
                state = pg['metric']['state']
                pg_total += 1
                if 'active+clean' == state:
                    healthy += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    unactive += 1
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    recovering += 1
                elif ('degraded' in state) or ('undersized' in state):
                    degraded += 1

            cluster_pg_state = {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total, 3) if pg_total
                else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}
        except Exception:
            logger.error("Failed to get cluster pg state")

        return cluster_pg_state

    def pool_get_perf(self, pool):
        for m in pool_perf:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_metric(
                metric, filter={"pool_id": pool.pool_id,
                                'cluster_id': pool.cluster_id})
            pool.metrics.update({m: value})

    def pool_get_histroy_perf(self, pool, start, end, metrics):
        for m in pool_perf:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "pool_id": pool.pool_id,
                    'cluster_id': pool.cluster_id})
            metrics.update({m: value})

    def pool_get_capacity(self, pool):
        for m in pool_capacity:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_metric(
                metric, filter={"pool_id": pool.pool_id,
                                'cluster_id': pool.cluster_id})
            pool.metrics.update({m: value})
        total_map = pool_total_perf_map
        for pool_key in total_map:
            value = self.prometheus_get_metric(
                total_map[pool_key], filter={'pool_id': pool.pool_id,
                                             'cluster_id': pool.cluster_id})
            pool.metrics.update({pool_key: value})

    def pool_get_histroy_capacity(self, pool, start, end, metrics):
        for m in pool_capacity:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "pool_id": pool.pool_id,
                    'cluster_id': pool.cluster_id
                })
            metrics.update({m: value})

    def pool_get_pg_state(self, pool):
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            pg_value = json.loads(
                prometheus.query(
                    metric='ceph_pg_metadata', filter={
                        'pool_id': pool.pool_id,
                        'cluster_id': pool.cluster_id
                    }))['data']['result']
            healthy = 0
            degraded = 0
            recovering = 0
            unactive = 0
            pg_total = 0.0
            for pg in pg_value:
                state = pg['metric']['state']
                pg_total += 1
                if 'active+clean' == state:
                    healthy += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    unactive += 1
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    recovering += 1
                elif ('degraded' in state) or ('undersized' in state):
                    degraded += 1

            pool.metrics.update({'pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total,
                                    3) if pg_total else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except exception.StorException as e:
            logger.error(e)
        except Exception as e:
            logger.error(e)
            pool.metrics.update({'pg_state': None})

    def osd_get_capacity(self, osd):
        logger.info("osd_get_capacity: osd_id: %s.", osd.id)
        for m in osd_capacity:
            metric = "ceph_osd_capacity_" + m
            value = self.prometheus_get_metric(metric, filter={
                "osd_id": int(osd.osd_id or '-1'),
                'cluster_id': osd.cluster_id
            })
            osd.metrics.update({m: value})

    def osds_get_capacity(self, osds):
        prometheus = PrometheusClient(url=self.prometheus_url)
        logger.info("osds get capacity")
        filters = {
            'cluster_id': self.ctxt.cluster_id
        }
        capacitys = {}
        for osd in osds:
            capacitys[osd.osd_id] = osd
        for m in osd_capacity:
            metric = "ceph_osd_capacity_" + m
            try:
                values = json.loads(
                    prometheus.query(metric=metric, filter=filters))
            except Exception as e:
                logger.error("Get prometheus error: %s", e)
                return None
            values = values['data']['result']
            for value in values:
                osd_id = value['metric']['osd_id']
                v = value['value']
                if osd_id in capacitys.keys():
                    osd = capacitys[osd_id]
                    osd.metrics.update({m: v})

    def osd_get_bluefs_capacity(self, osd):
        logger.info("osd_get_bluefs_capacity: osd_id: %s.", osd.id)
        for m in bluefs_capacity:
            metric = "ceph_bluefs_" + m
            value = self.prometheus_get_metric(metric, filter={
                "ceph_daemon": "osd.{}".format(osd.osd_id),
                'cluster_id': osd.cluster_id
            })
            osd.metrics.update({m: value})

    def osd_get_histroy_capacity(self, osd, start, end, metrics):
        for m in osd_capacity:
            metric = "ceph_osd_capacity_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "osd_id": int(osd.osd_id or '-1'),
                    'cluster_id': osd.cluster_id
                })
            metrics.update({m: value})

    def osd_get_realtime_metrics(self, osd):
        for m in osd_rate:
            metric = "ceph_osd_rate_" + m
            value = self.prometheus_get_metric(metric, filter={
                "osd_id": int(osd.osd_id or '-1'),
                'cluster_id': osd.cluster_id
            })
            osd.metrics.update({m: value})
        self.osd_get_capacity(osd)
        return osd.metrics

    def osd_get_histroy_metrics(self, osd, start, end):
        metrics = {}
        for m in osd_rate:
            metric = "ceph_osd_rate_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "osd_id": int(osd.osd_id or '-1'),
                    'cluster_id': osd.cluster_id
                })
            metrics.update({m: value})
        self.osd_get_histroy_capacity(osd, start, end, metrics)
        return metrics

    def osd_disk_perf(self, osd):
        disk = objects.Disk.get_by_id(self.ctxt, osd.disk_id)
        node = objects.Node.get_by_id(self.ctxt, disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': disk.name,
                                       'cluster_id': osd.cluster_id})
            osd.metrics.update({m: data})
            if osd.cache_partition:
                data = self.get_node_exporter_metric(
                    metric_method,
                    filter={
                        'hostname': node.hostname,
                        'device': osd.cache_partition})
                osd.metrics.update({"cache_{}".format(m): data})

    def osds_disk_perf(self, osds):
        disk_perf = {}
        for osd in osds:
            if not disk_perf.get(osd.node.hostname):
                disk_perf[osd.node.hostname] = {}
            disk_perf[osd.node.hostname][osd.disk.name] = osd
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            datas = self.get_node_exporter_metrics(
                metric_method, filter={'cluster_id': self.ctxt.cluster_id})
            if not datas:
                continue
            for data in datas:
                disk_name = data['metric']['device']
                hostname = data['metric']['hostname']
                value = data['value']
                if hostname in disk_perf.keys():
                    osd = disk_perf[hostname].get(disk_name)
                    if osd:
                        osd.metrics.update({m: value})

    def osd_disk_histroy_metircs(self, osd, start, end, metrics):
        disk = objects.Disk.get_by_id(self.ctxt, osd.disk_id)
        node = objects.Node.get_by_id(self.ctxt, disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname,
                        'device': disk.name,
                        'cluster_id': osd.cluster_id},
                graph=True, start=start, end=end)
            metrics.update({m: data})
            if osd.cache_partition:
                data = self.get_node_exporter_metric(
                    metric_method,
                    filter={
                        'hostname': node.hostname,
                        'device': osd.cache_partition},
                    graph=True,
                    start=start,
                    end=end)
                metrics.update({"cache_{}".format(m): data})

    def osds_get_pg_state(self, osds):
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            filters = {
                'cluster_id': self.ctxt.cluster_id
            }
            pg_value = json.loads(prometheus.query(
                metric='ceph_pg_metadata', filter=filters))['data']['result']
            pg_state = {}
        except Exception as e:
            logger.error("Get prometheus error: ", e)
            return

        for osd in osds:
            osd.metrics.update({'pg_state': None})
        try:
            for osd in osds:
                pg_state[osd.osd_id] = {
                    'osd': osd,
                    'healthy': 0,
                    'degraded': 0,
                    'recovering': 0,
                    'unactive': 0,
                    'pg_total': 0.0
                }
            for pg in pg_value:
                osd_id = pg['metric']['osd_id']
                state = pg['metric']['state']
                if osd_id not in pg_state.keys():
                    continue
                pg_state[osd_id]['pg_total'] += 1
                if 'active+clean' == state:
                    pg_state[osd_id]['healthy'] += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    pg_state[osd_id]['unactive'] += 1
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    pg_state[osd_id]['recovering'] += 1
                elif ('degraded' in state) or ('undersized' in state):
                    pg_state[osd_id]['degraded'] += 1

            for osd_id, state in six.iteritems(pg_state):
                osd = state['osd']
                healthy = state['healthy']
                recovering = state['recovering']
                degraded = state['degraded']
                unactive = state['unactive']
                pg_total = state['pg_total']
                osd.metrics.update({'pg_state': {
                    'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                    'recovering': round(recovering / pg_total,
                                        3) if pg_total else 0,
                    'degraded': round(degraded / pg_total,
                                      3) if pg_total else 0,
                    'unactive': round(unactive / pg_total,
                                      3) if pg_total else 0}})
        except Exception as e:
            logger.error("Get osds_get_pg_state error", e)

    def cluster_get_capacity(self, filter=None):
        # ????????????
        logger.info('get cluster capacity')
        metrics = {
            'total_bytes': 'ceph_cluster_total_bytes',
            'total_used_bytes': 'ceph_cluster_total_used_bytes',
            'total_avail_bytes':
                'ceph_cluster_total_bytes - ceph_cluster_total_used_bytes',
            'total_provisioned': 'ceph_cluster_provisioned_capacity'
        }
        cluster_capacity = {}
        for k, v in six.iteritems(metrics):
            value = self.prometheus_get_metric(v, filter=filter)
            cluster_capacity[k] = value
        return cluster_capacity

    def prometheus_get_list_metrics(self, metric, filter=None):
        """Get metrics from prometheus
        Returns: list datas
        """
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query(metric=metric, filter=filter))
        except Exception as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            return None
        data = value['data']['result']
        if data:
            logger.info('get metric:%s data success from prometheus, data:%s',
                        metric, data)
            return data
        else:
            logger.info('get metric:%s data is None from prometheus', metric)
            return None

    def pool_get_provisioned_capacity(self, ctxt, pool_id):
        # pool???????????????????????????
        logger.info('get pool_id:%s capacity', pool_id)
        metrics = {
            'total_avail_bytes': 'ceph_pool_max_avail',
            'total_used_bytes': 'ceph_pool_bytes_used',
            'total_bytes': 'ceph_pool_max_avail + ceph_pool_bytes_used',
            'total_provisioned': 'ceph_pool_provisioned_capacity'
        }
        result = {}
        for k, v in six.iteritems(metrics):
            value = self.prometheus_get_metric(
                v, filter={'pool_id': pool_id, 'cluster_id': ctxt.cluster_id})
            result[k] = value
        return result

    def disk_io_top(self, ctxt, k):
        m = ('topk({}, rate(node_disk_io_now{{cluster_id="{}",'
             ' device!~"{}"}}[5m]))'.format(k, ctxt.cluster_id, DISK_SKIP))
        logger.info('disk io top query: %s', m)
        datas = self.prometheus_get_metrics(m)
        if not datas:
            return None
        if len(datas) > k:
            datas = datas[:k]
        disks = []
        for data in datas:
            disks.append({
                "hostname": data['metric']['hostname'],
                "name": data['metric']['device'],
                "value": data['value'][1]
            })
        return disks

    def disks_io_util(self, ctxt):
        logger.info('disk io utils')
        m = ('irate(node_disk_io_time_seconds_total{{cluster_id="{}",'
             ' device!~"{}"}}[5m])').format(ctxt.cluster_id, DISK_SKIP)
        logger.info('disk io top query: %s', m)
        datas = self.prometheus_get_metrics(m)
        if not datas:
            return None
        disks = []
        for data in datas:
            disks.append({
                "hostname": data['metric']['hostname'],
                "name": data['metric']['device'],
                "value": data['value'][1]
            })
        return disks

    def object_user_get_capacity(self, obj_user, metrics):
        for m in object_user_capacity_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_metric(
                metric, filter={"uid": obj_user.uid,
                                'cluster_id': obj_user.cluster_id})
            metrics.update({m: value})

    def object_user_get_perf(self, object_user, metrics):
        uid = object_user.uid
        cluster_id = object_user.cluster_id
        for m in object_user_perf_attrs:
            metric = "ceph_" + m
            filters = "uid='{}', cluster_id='{}'".format(uid, cluster_id)
            irate_metrics = 'rate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_metric(irate_metrics, not_filter=True)
            metrics.update({m: value})

    def object_user_get_histroy_capacity(self, obj_user, start, end, metrics):
        for m in object_user_capacity_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "uid": obj_user.uid,
                    "cluster_id": obj_user.cluster_id
                })
            metrics.update({m: value})

    def object_user_get_histroy_perf(self, obj_user, start, end, metrics):
        uid = obj_user.uid
        cluster_id = obj_user.cluster_id
        for m in object_user_perf_attrs:
            metric = "ceph_" + m
            filters = "uid='{}', cluster_id='{}'".format(uid, cluster_id)
            irate_metrics = 'rate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_histroy_metric(
                irate_metrics, start=start, end=end, not_filter=True)
            metrics.update({m: value})

    def object_bucket_get_capacity(self, object_bucket, metrics):
        bucket = object_bucket.name
        cluster_id = object_bucket.cluster_id
        owner = object_bucket.owner.uid
        for m in object_bucket_capacity_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_metric(
                metric, filter={"bucket": bucket, "cluster_id": cluster_id,
                                "owner": owner})
            metrics.update({m: value})

    def object_bucket_get_perf(self, object_bucket, metrics):
        bucket = object_bucket.name
        cluster_id = object_bucket.cluster_id
        owner = object_bucket.owner.uid
        for m in object_bucket_perf_attrs:
            metric = "ceph_" + m
            filters = "bucket='{}', cluster_id='{}', owner='{}'".format(
                bucket, cluster_id, owner)
            irate_metrics = 'rate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_metric(irate_metrics, not_filter=True)
            metrics.update({m: value})

    def object_bucket_get_histroy_capacity(self, obj_bucket, start, end,
                                           metrics):
        bucket = obj_bucket.name
        cluster_id = obj_bucket.cluster_id
        owner = obj_bucket.owner.uid
        for m in object_bucket_capacity_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "bucket": bucket, "cluster_id": cluster_id, "owner": owner
                })
            metrics.update({m: value})

    def object_bucket_get_histroy_perf(self, obj_bucket, start, end, metrics):
        bucket = obj_bucket.name
        cluster_id = obj_bucket.cluster_id
        owner = obj_bucket.owner.uid
        for m in object_bucket_perf_attrs:
            metric = "ceph_" + m
            filters = "bucket='{}', cluster_id='{}', owner='{}'".format(
                bucket, cluster_id, owner)
            irate_metrics = 'rate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_histroy_metric(
                irate_metrics, start=start, end=end, not_filter=True)
            metrics.update({m: value})

    def radosgw_get_cpu_memory(self, rgw, metrics):
        for m in rgw_gateway_cpu_memory_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_metric(
                metric, filter={"ceph_daemon": rgw.name,
                                "cluster_id": rgw.cluster_id})
            metrics.update({m: value})

    def radosgw_get_perf(self, rgw, metrics):
        cluster_id = rgw.cluster_id
        for m in rgw_gateway_perf_attrs:
            metric = "ceph_" + m
            filters = "ceph_daemon='rgw.{}', cluster_id='{}'".format(
                rgw.name, cluster_id)
            irate_metrics = 'irate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_metric(irate_metrics, not_filter=True)
            metrics.update({m: value})

    def radosgw_get_histroy_cpu_memory(self, rgw, start, end, metrics):
        for m in rgw_gateway_cpu_memory_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "ceph_daemon": rgw.name,
                    "cluster_id": rgw.cluster_id})
            metrics.update({m: value})

    def radosgw_get_histroy_perf(self, rgw, start, end, metrics):
        cluster_id = rgw.cluster_id
        for m in rgw_gateway_perf_attrs:
            metric = "ceph_" + m
            filters = "ceph_daemon='rgw.{}', cluster_id='{}'".format(
                rgw.name, cluster_id)
            irate_metrics = 'irate({metric}{{{filters}}}[1m])'.format(
                metric=metric, filters=filters)
            value = self.prometheus_get_histroy_metric(
                irate_metrics, start=start, end=end, not_filter=True)
            metrics.update({m: value})

    def rgw_router_get_cpu_memory(self, rgw_router, metrics, service_name,
                                  hostname):
        for m in rgw_router_cpu_memory_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_metric(
                metric, filter={"hostname": hostname,
                                "service_name": service_name,
                                "cluster_id": rgw_router.cluster_id})
            metrics.update({m: value})

    def rgw_router_get_histroy_cpu_memory(self, rgw_router, start, end,
                                          metrics, service_name, hostname):
        for m in rgw_router_cpu_memory_attrs:
            metric = "ceph_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "hostname": hostname,
                    "service_name": service_name,
                    "cluster_id": rgw_router.cluster_id})
            metrics.update({m: value})

    def object_bucket_get_bandwidth_total(self, object_bucket, metrics):
        bucket = object_bucket.name
        cluster_id = object_bucket.cluster_id
        owner = object_bucket.owner.uid
        for m in [MeK.BUCKET_SENT_NUM, MeK.BUCKET_RECEIVED_NUM]:
            metric = "ceph_" + m
            value = self.prometheus_get_metric(
                metric, filter={"bucket": bucket, "cluster_id": cluster_id,
                                "owner": owner})
            metrics.update({m: value})
