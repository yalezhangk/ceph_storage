from __future__ import print_function
import logging

import grpc

from . import scheduler_pb2
from . import scheduler_pb2_grpc

from ..service import BaseClient
from ..service import BaseClientManager


class SchedulerClient(BaseClient):

    def get_ceph_conf(self, ceph_host=None, location=None):
        response = self._stub.GetCephConf(
            scheduler_pb2.StorageLocation(location=location))
        return response.content

    def AppendCephMonitor(self, location=None):
        response = self._stub.AppendCephMonitor(
            scheduler_pb2.SRequest(key=location))
        return response.value


class SchedulerClientManager(BaseClientManager):
    service_name = "scheduler"
    client_cls = SchedulerClient

    def register_stub(self, channel):
        return scheduler_pb2_grpc.SchedulerStub(channel)


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = SchedulerClientManager().get_client("whx-ceph-1")
    print(client.get_ceph_conf())
    print(client.AppendCephMonitor())

