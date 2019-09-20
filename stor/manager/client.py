from __future__ import print_function
import logging


from stor.service import BaseClient
from stor.service import BaseClientManager


class ManagerClient(BaseClient):

    def get_ceph_conf(self, ceph_host=None, location=None):
        response = self._stub.GetCephConf()
        return response.content

    def AppendCephMonitor(self, location=None):
        response = self._stub.AppendCephMonitor()
        return response.value

    def volume_get_all(self, marker, limit, sort_keys,
                       sort_dirs, filters, offset):
        response = self._stub.VolumeGetAll()
        return response.value


class ManagerClientManager(BaseClientManager):
    service_name = "manager"
    client_cls = ManagerClient


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = ManagerClientManager().get_client("whx-ceph-1")
    print(client.get_ceph_conf())
    print(client.AppendCephMonitor())
