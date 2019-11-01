import json

from prometheus_http_client import NodeExporter
from prometheus_http_client import Prometheus as PrometheusClient
from t2stor import exception
from t2stor import objects


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
                    "cluster_id": self.ctxt.cluster_id,
                    "service_name": 'prometheus'
                }
            )
            if not endpoints:
                raise exception.PrometheusEndpointNotFound(
                    service_name='prometheus', cluster_id=self.ctxt.cluster_id)
            endpoint = json.loads(endpoints[0].endpoint)
            self.prometheus_url = "http://{}:{}".format(endpoint['ip'],
                                                        endpoint['port'])

    def _get_sys_disk(self, node_id):
        return objects.DiskList.get_all(
            self.ctxt, filters={'node_id': node_id, 'role': 'system'})[0]

    def _get_net_name(self, node_id, ipaddr):
        network = objects.NetworkList.get_all(self.ctxt, filters={
            'node_id': node_id,
            'ip_address': ipaddr
        })[0]
        return network.name

    def prometheus_get_metric(self, metric, filter=None):
        """Get metrics from prometheus
        Returns: metrics value
        """
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            value = json.loads(prometheus.query(metric=metric, filter=filter))
        except BaseException:
            return None

        if len(value['data']['result']):
            return value['data']['result'][0]['value']
        else:
            return None

    def prometheus_get_histroy_metric(self, metric, start, end, filter=None):
        """Get metrics from prometheus
        Returns: metrics value
        """
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
        node_exporter = NodeExporter(url=self.prometheus_url)

        function = getattr(node_exporter, metric)

        graph = kwargs.get('graph')
        try:
            value = json.loads(function(**kwargs))
        except BaseException:
            return None

        if len(value['data']['result']):
            if graph:
                return value['data']['result'][0]['values']
            else:
                return value['data']['result'][0]['value']
        else:
            return None

    def node_get_metrics_network(self, node, net_name):

        for metric in node.network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': net_name})
            setattr(node, metric, data)

    def node_get_metrics_monitor(self, node):
        sys_disk_name = self._get_sys_disk(node.id)
        node.cpu_rate = self.get_node_exporter_metric(
            "node_cpu_rate", filter={'hostname': node.hostname})
        node.memory_rate = self.get_node_exporter_metric(
            "node_memory_rate", filter={'hostname': node.hostname})
        node.load5 = self.get_node_exporter_metric("node_load5", filter={
            'hostname': node.hostname})
        node.disk_io_rate = self.get_node_exporter_metric(
            "node_disk_io_rate", filter={'hostname': node.hostname,
                                         'device': sys_disk_name})

        net_name = self._get_net_name(node.id, node.ip_address)
        for metric in node.network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': net_name})
            setattr(node, metric, data)

    def node_get_metrics_histroy_monitor(self, node, start, end):
        sys_disk_name = self._get_sys_disk(node.id)
        node.cpu_rate = self.get_node_exporter_metric(
            "node_cpu_rate", filter={'hostname': node.hostname},
            graph=True, start=start, end=end)
        node.memory_rate = self.get_node_exporter_metric(
            "node_memory_rate", filter={'hostname': node.hostname},
            graph=True, start=start, end=end)
        node.load5 = self.get_node_exporter_metric(
            "node_load5", filter={'hostname': node.hostname},
            graph=True, start=start, end=end)
        node.disk_io_rate = self.get_node_exporter_metric(
            "node_disk_io_rate",
            filter={'hostname': node.hostname, 'device': sys_disk_name},
            graph=True, start=start, end=end)

        net_name = self._get_net_name(node.id, node.ip_address)
        for metric in node.network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname, 'device': net_name},
                graph=True, start=start, end=end)
            setattr(node, metric, data)

    def node_get_metrics_histroy_network(self, node, net_name, start, end):
        for metric in node.network_attrs:
            metric_method = 'node_{}'.format(metric)
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname, 'device': net_name},
                graph=True, start=start, end=end)
            setattr(node, metric, data)

    def node_get_metrics_overall(self, node):
        net_name = self._get_net_name(node.id, node.ip_address)
        sys_disk_name = self._get_sys_disk(node.id)

        for metric in node.prometheus_attrs:
            metric_method = 'node_{}'.format(metric)
            if metric in node.network_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname,
                        'device': net_name})
            elif metric in node.disk_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname, 'device': sys_disk_name})
            elif metric in node.fs_attrs:
                data = self.get_node_exporter_metric(
                    metric_method, filter={
                        'hostname': node.hostname, 'mountpoint': '/'})
            else:
                data = self.get_node_exporter_metric(metric_method, filter={
                    'hostname': node.hostname})
            node.metrics.update({metric: data})

    def disk_get_perf(self, disk):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate',
                        'read_bytes_rate', 'write_lat_rate', 'read_lat_rate',
                        'io_rate']
        node = objects.Node.get_by_id(disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(metric_method,
                                                 filter={
                                                     'hostname': node.hostname,
                                                     'device': disk.name})
            disk.metrics.update({m: data})

    def disk_get_histroy_perf(self, disk, start, end):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate',
                        'read_bytes_rate', 'write_lat_rate', 'read_lat_rate',
                        'io_rate']
        node = objects.Node.get_by_id(disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname, 'device': disk.name},
                graph=True, start=start, end=end)
            disk.metrics.update({m: data})

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
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    recovering += 1
                elif ('degraded' in state) or ('undersized' in state):
                    degraded += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    unactive += 1

            cluster_metrics.update({'cluster_pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total, 3) if pg_total
                else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except BaseException:
            cluster_metrics.update({'cluster_pg_state': None})

    def pool_get_perf(self, pool):
        for m in pool.pool_perf:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_metric(
                metric, filter={"pool_id": pool.pool_id})
            pool.metrics.update({m: value})

    def pool_get_histroy_perf(self, pool, start, end):
        for m in pool.pool_perf:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "pool_id": pool.pool_id})
            pool.metrics.update({m: value})

    def pool_get_capacity(self, pool):
        for m in pool.pool_capacity:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_metric(
                metric, filter={"pool_id": pool.pool_id})
            pool.metrics.update({m: value})
        total_map = pool.pool_total_perf_map
        for pool_key in total_map:
            value = self.prometheus_get_metric(
                total_map[pool_key], filter={'pool_id': pool.pool_id})
            pool.metrics.update({pool_key: value})

    def pool_get_histroy_capacity(self, pool, start, end):
        for m in pool.pool_capacity:
            metric = "ceph_pool_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "pool_id": pool.pool_id})
            pool.metrics.update({m: value})

    def pool_get_pg_state(self, pool):
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            pg_value = json.loads(
                prometheus.query(
                    metric='ceph_pg_metadata', filter={
                        'pool_id': pool.pool_id}))['data']['result']
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
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    recovering += 1
                elif ('degraded' in state) or ('undersized' in state):
                    degraded += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    unactive += 1

            pool.metrics.update({'pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total,
                                    3) if pg_total else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except BaseException:
            pass

    def osd_get_capacity(self, osd):
        for m in osd.osd_capacity:
            metric = "ceph_osd_capacity_" + m
            value = self.prometheus_get_metric(metric, filter={
                "osd_id": int(osd.name or '-1')})
            osd.metrics.update({m: value})

    def osd_get_bluefs_capacity(self, osd):
        for m in osd.bluefs_capacity:
            metric = "ceph_bluefs_" + m
            value = self.prometheus_get_metric(metric, filter={
                "ceph_daemon": "osd.{}".format(osd.name)})
            osd.metrics.update({m: value})

    def osd_get_histroy_capacity(self, osd, start, end):

        for m in osd.osd_capacity:
            metric = "ceph_osd_capacity_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "osd_id": int(osd.name or '-1')
                })
            osd.metrics.update({m: value})

    def osd_get_realtime_metrics(self, osd):
        for m in osd.osd_rate:
            metric = "ceph_osd_rate_" + m
            value = self.prometheus_get_metric(metric, filter={
                "osd_id": int(osd.name or '-1')})
            osd.metrics.update({m: value})
        self.osd_get_capacity(osd)

    def osd_get_histroy_metrics(self, osd, start, end):

        for m in osd.osd_rate:
            metric = "ceph_osd_rate_" + m
            value = self.prometheus_get_histroy_metric(
                metric, start=start, end=end, filter={
                    "osd_id": int(osd.name or '-1')})
            osd.metrics.update({m: value})
        self.osd_get_histroy_capacity(osd)

    def osd_disk_perf(self, osd):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate', 'read_bytes_rate',
                        'write_lat_rate', 'read_lat_rate',
                        'io_rate']
        disk = objects.Disk.get_by_id(osd.disk_id)
        node = objects.Node.get_by_id(disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method, filter={'hostname': node.hostname,
                                       'device': disk.name})
            osd.metrics.update({m: data})
            if osd.cache_partition:
                data = self.get_node_exporter_metric(
                    metric_method,
                    filter={
                        'hostname': node.hostname,
                        'device': osd.cache_partition})
                osd.metrics.update({"cache_{}".format(m): data})

    def osd_disk_histroy_perf(self, osd, start, end):
        disk_metrics = ['write_iops_rate', 'read_iops_rate',
                        'write_bytes_rate', 'read_bytes_rate',
                        'write_lat_rate', 'read_lat_rate',
                        'io_rate']
        disk = objects.Disk.get_by_id(osd.disk_id)
        node = objects.Node.get_by_id(disk.node_id)
        for m in disk_metrics:
            metric_method = "node_disk_" + m
            data = self.get_node_exporter_metric(
                metric_method,
                filter={'hostname': node.hostname, 'device': disk.name},
                graph=True, start=start, end=end)
            osd.metrics.update({m: data})
            if osd.cache_partition:
                data = self.get_node_exporter_metric(
                    metric_method,
                    filter={
                        'hostname': node.hostname,
                        'device': osd.cache_partition},
                    graph=True,
                    start=start,
                    end=end)
                osd.metrics.update({"cache_{}".format(m): data})

    def osd_get_pg_state(self, osd):
        prometheus = PrometheusClient(url=self.prometheus_url)
        try:
            pg_value = json.loads(prometheus.query(
                metric='ceph_pg_metadata', filter={
                    'osd_id': int(osd.name or '-1')}))['data']['result']
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
                elif ('recover' in state) or ('backfill' in state) or (
                        'peer' in state) or ('remapped' in state):
                    recovering += 1
                elif ('degraded' in state) or ('undersized' in state):
                    degraded += 1
                elif ('unactive' in state) or ('stale' in state) or (
                        'down' in state) or ('unknown' in state):
                    unactive += 1

            osd.metrics.update({'pg_state': {
                'healthy': round(healthy / pg_total, 3) if pg_total else 0,
                'recovering': round(recovering / pg_total,
                                    3) if pg_total else 0,
                'degraded': round(degraded / pg_total, 3) if pg_total else 0,
                'unactive': round(unactive / pg_total, 3) if pg_total else 0}})
        except BaseException:
            pass
