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
        }

        self.role_monitor = {
            "MON": "ceph-mon@$HOSTNAME",
            "MGR": "ceph-mgr@$HOSTNAME",
        }

        self.role_block_gateway = {
            "TCMU": "tcmu",
        }
