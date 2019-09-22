from __future__ import print_function
import logging

from stor import objects
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

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "volume_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def volume_get(self, ctxt, volume_id):
        response = self.call(ctxt, "volume_get", volume_id=volume_id)
        return response


class ManagerClientManager(BaseClientManager):
    cluster = "default"
    service_name = "manager"
    client_cls = ManagerClient


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    objects.register_all()
    client = ManagerClientManager(cluster_id='7be530ce').get_client("devel")
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    print(client.get_ceph_conf(ctxt))
    print(client.volume_get_all(ctxt))
    print(client.volume_get(ctxt, "0601c6dc-39eb-4b9f-af19-0c50268d39e9"))
