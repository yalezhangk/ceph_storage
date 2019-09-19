from __future__ import print_function
import logging

from stor.porto import stor_pb2
from stor.porto import stor_pb2_grpc

from stor.service import BaseClient
from stor.service import BaseClientManager


class ManagerClient(BaseClient):

    def get_ceph_conf(self, ceph_host=None, location=None):
        response = self._stub.GetCephConf(
            stor_pb2.StorageLocation(location=location))
        return response.content

    def AppendCephMonitor(self, location=None):
        response = self._stub.AppendCephMonitor(
            stor_pb2.Request(key=location))
        return response.value

    def volume_get_all(self, marker, limit, sort_keys,
                       sort_dirs, filters, offset):
        response = self._stub.AppendCephMonitor(
            stor_pb2.ListRequest(marker, limit, sort_keys,
                                 sort_dirs, filters, offset))
        return response.value


class ManagerClientManager(BaseClientManager):
    service_name = "manager"
    client_cls = ManagerClient

    def register_stub(self, channel):
        return stor_pb2_grpc.ManagerStub(channel)


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = ManagerClientManager().get_client("whx-ceph-1")
    print(client.get_ceph_conf())
    print(client.AppendCephMonitor())
