from __future__ import print_function
import logging
import json

from stor.proto import stor_pb2
from stor.proto import stor_pb2_grpc
from stor.service import BaseClient
from stor.service import BaseClientManager


logger = logging.getLogger(__name__)


class AgentClient(BaseClient):
    service_name = "agent"

    def get_host_disks(self):
        response = self._stub.GetDiskInfo(stor_pb2.Request(key='you'))
        return json.loads(response.value)

    def install_package(self):
        response = self._stub.InstallPackage(stor_pb2.Request(key='you'))
        return response.value

    def start_service(self):
        response = self._stub.StartService(stor_pb2.Request(key='you'))
        return response.value

    def write_ceph_conf(self):
        response = self._stub.WriteCephConf(stor_pb2.Request(key='you'))
        return response.value


class AgentClientManager(BaseClientManager):
    service_name = "agent"
    client_cls = AgentClient

    def register_stub(self, channel):
        return stor_pb2_grpc.AgentStub(channel)


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = AgentClientManager().get_client("whx-ceph-1")
    res = client.get_host_disks()
    logger.info(res)
