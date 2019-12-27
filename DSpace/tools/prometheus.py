import json
import logging
import time

import six
from prometheus_http_client import NodeExporter
from prometheus_http_client import Prometheus as PrometheusClient
from prometheus_http_client.prometheus import relabel

from DSpace import exception
from DSpace import objects


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

sys_disk_used_rate_attrs = ['sys_disk_used_rate']
sys_disk_size_attrs = ['sys_disk_size']

disk_attrs = ['disk_io_rate']

network_attrs = ['network_transmit_packets_rate',
                 'network_receive_packets_rate',
                 'network_transmit_rate', 'network_receive_rate',
                 'network_errs_rate', 'network_drop_rate']

prometheus_attrs = cpu_attrs + sys_attrs + disk_attrs + network_attrs +\
    sys_disk_used_rate_attrs + sys_disk_size_attrs

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

    def prometheus_get_metric(self, metric, filter=None):
        """Get metrics from prometheus
        Returns: metrics value
        """
        filter = filter or {}
        if "cluster_id" not in filter.keys():
            filter['cluster_id'] = self.ctxt.cluster_id
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query(metric=metric, filter=filter))
        except BaseException as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            raise e

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
        except BaseException as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            raise e

        if len(value['data']['result']):
            data = value['data']['result']
            logger.info('get metric:%s data success from prometheus, data:%s',
                        metric, data)
            return data
        else:
            logger.info('get metric:%s data is None from prometheus', metric)
            return None

    def prometheus_get_histroy_metric(self, metric, start, end, filter=None):
        """Get metrics from prometheus
        Returns: metrics value
        """
        filter = filter or {}
        if "cluster_id" not in filter.keys():
            filter['cluster_id'] = self.ctxt.cluster_id
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query_rang(
                metric=metric, filter=filter, start=start, end=end))
        except BaseException:
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
        except BaseException:
            if graph:
                return None
            else:
                return [time.time(), 0]

        if len(value['data']['result']):
            if graph:
                return value['data']['result'][0]['values']
            else:
                return value['data']['result'][0]['value']
        else:
            if graph:
                return None
            else:
                return [time.time(), 0]

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
            elif metric in sys_disk_used_rate_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'mountpoint': '/',
                        'cluster_id': node.cluster_id})
            elif metric in sys_disk_size_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'mountpoint': '/',
                        'cluster_id': node.cluster_id})
            else:
                data = self.get_node_exporter_metric(metric_method, filter={
                    'hostname': node.hostname, 'cluster_id': node.cluster_id})
            node.metrics.update({metric: data})

    def disk_get_perf(self, disk):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate',
                        'read_bytes_rate', 'write_lat_rate', 'read_lat_rate',
                        'io_rate']
        node = objects.Node.get_by_id(self.ctxt, disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': disk.name,
                                       'cluster_id': disk.cluster_id})
            disk.metrics.update({m: data})

    def disk_get_histroy_perf(self, disk, start, end):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate',
                        'read_bytes_rate', 'write_lat_rate', 'read_lat_rate',
                        'io_rate']
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

    def cluster_get_pg_state(self, cluster_metrics):
        prometheus = PrometheusClient(url=self.prometheus_url)
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

            cluster_metrics.update({'cluster_pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total, 3) if pg_total
                else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except BaseException:
            cluster_metrics.update({'cluster_pg_state': None})

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

    def osd_get_capacity(self, osd):
        logger.info("osd_get_capacity: osd_id: %s.", osd.id)
        for m in osd_capacity:
            metric = "ceph_osd_capacity_" + m
            value = self.prometheus_get_metric(metric, filter={
                "osd_id": int(osd.osd_id or '-1'),
                'cluster_id': osd.cluster_id
            })
            osd.metrics.update({m: value})

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
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate', 'read_bytes_rate',
                        'write_lat_rate', 'read_lat_rate',
                        'io_rate']
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

    def osd_disk_histroy_metircs(self, osd, start, end, metrics):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate', 'read_bytes_rate',
                        'write_lat_rate', 'read_lat_rate',
                        'io_rate']
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

    def osd_get_pg_state(self, osd):
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            filters = {
                'osd_id': int(osd.osd_id or '-1'),
                'cluster_id': osd.cluster_id
            }
            pg_value = json.loads(prometheus.query(
                metric='ceph_pg_metadata', filter=filters))['data']['result']
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

            osd.metrics.update({'pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total,
                                    3) if pg_total else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except exception.StorException as e:
            logger.error(e)
            pass

    def cluster_get_capacity(self, filter=None):
        # 集群容量
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
        except BaseException as e:
            logger.error('get metric:%s data error from prometheus:%s',
                         metric, e)
            raise e
        data = value['data']['result']
        if data:
            logger.info('get metric:%s data success from prometheus, data:%s',
                        metric, data)
            return data
        else:
            logger.info('get metric:%s data is None from prometheus', metric)
            return None

    def pool_get_provisioned_capacity(self, ctxt, pool_id):
        # pool容量和已配置的容量
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
