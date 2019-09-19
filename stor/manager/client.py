from __future__ import print_function
import logging


from stor.service import BaseClient
from stor.service import BaseClientManager
from stor.context import RequestContext


class ManagerClient(BaseClient):

    def get_ceph_conf(self, ctxt, ceph_host=None):
        response = self.call(ctxt, method="get_ceph_conf", ceph_host=ceph_host)
        return response

    def AppendCephMonitor(self, location=None):
        response = self._stub.AppendCephMonitor()
        return response.value

    def volume_get_all(self, marker, limit, sort_keys,
                       sort_dirs, filters, offset):
        response = self._stub.VolumeGetAll()
        return response.value


class ManagerClientManager(BaseClientManager):
    cluster = "default"
    service_name = "manager"
    client_cls = ManagerClient


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = ManagerClientManager().get_client("devel")
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    print(client.get_ceph_conf(ctxt))
    # print(client.AppendCephMonitor())
