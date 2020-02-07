class ServiceMap(object):

    def __init__(self, namspace):
        self.container_namespace = namspace

        self.container_roles = ["base", "role_admin", "role_radosgw_router"]

        self.debug_services = [
            self.container_namespace + "_dsm",
            self.container_namespace + "_dsi",
            self.container_namespace + "_dsa",
        ]

        self.base = {
            "NODE_EXPORTER": self.container_namespace + "_node_exporter",
            "CHRONY": self.container_namespace + "_chrony",
            "DSA": self.container_namespace + "_dsa",
        }

        self.role_admin = {
            "PROMETHEUS": self.container_namespace + "_prometheus",
            "ETCD": self.container_namespace + "_etcd",
            "NGINX": self.container_namespace + "_nginx",
            "DSM": self.container_namespace + "_dsm",
            "DSI": self.container_namespace + "_dsi",
            "MARIADB": self.container_namespace + "_mariadb",
            "REDIS": self.container_namespace + "_redis",
            "REDIS_SENTINEL": self.container_namespace + "_redis_sentinel",
            "TOOLBOX": self.container_namespace + "_toolbox",
        }

        self.role_monitor = {
            "MON": "ceph-mon@$HOSTNAME.service",
            "MGR": "ceph-mgr@$HOSTNAME.service",
            "MDS": "ceph-mds@$HOSTNAME.service",
        }

        self.role_block_gateway = {
            "TCMU": "tcmu",
        }

        self.map = {}
        self.map.update(self.base)
        self.map.update(self.role_admin)
        self.map.update(self.role_monitor)
        self.map.update(self.role_block_gateway)

    def get_service_name(self, name):
        service_name = self.map.get(name)
        return service_name
