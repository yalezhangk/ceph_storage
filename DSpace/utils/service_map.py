class ServiceMap(object):

    def __init__(self, prefix):
        self.container_prefix = prefix

        self.container_roles = ["base", "role_admin", "role_radosgw_router",
                                "role_block_gateway"]

        self.debug_services = [
            self.container_prefix + "_dsm",
            self.container_prefix + "_dsi",
            self.container_prefix + "_dsa",
        ]

        self.base = {
            "NODE_EXPORTER": self.container_prefix + "_node_exporter",
            "CHRONY": self.container_prefix + "_chrony",
            "DSA": self.container_prefix + "_dsa",
        }

        self.role_admin = {
            "PROMETHEUS": self.container_prefix + "_prometheus",
            "ETCD": self.container_prefix + "_etcd",
            "NGINX": self.container_prefix + "_nginx",
            "DSM": self.container_prefix + "_dsm",
            "DSI": self.container_prefix + "_dsi",
            "MARIADB": self.container_prefix + "_mariadb",
            "REDIS": self.container_prefix + "_redis",
            "REDIS_SENTINEL": self.container_prefix + "_redis_sentinel",
            "TOOLBOX": self.container_prefix + "_toolbox",
        }

        self.role_monitor = {
            "MON": "ceph-mon@$HOSTNAME.service",
            "MGR": "ceph-mgr@$HOSTNAME.service",
            "MDS": "ceph-mds@$HOSTNAME.service",
        }

        self.role_block_gateway = {
            "TCMU": self.container_prefix + "_tcmu_runner",
        }

        self.map = {}
        self.map.update(self.base)
        self.map.update(self.role_admin)
        self.map.update(self.role_monitor)
        self.map.update(self.role_block_gateway)

    def get_service_name(self, name):
        service_name = self.map.get(name)
        return service_name
