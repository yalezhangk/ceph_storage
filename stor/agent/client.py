from __future__ import print_function
import logging
import json

from stor.service import BaseClient
from stor.service import BaseClientManager


logger = logging.getLogger(__name__)


class AgentClient(BaseClient):
    service_name = "agent"

    def get_host_disks(self):
        response = self._stub.GetDiskInfo()
        return json.loads(response.value)

    def install_package(self):
        response = self._stub.InstallPackage()
        return response.value

    def start_service(self):
        response = self._stub.StartService()
        return response.value

    def write_ceph_conf(self):
        response = self._stub.WriteCephConf()
        return response.value


class AgentClientManager(BaseClientManager):
    service_name = "agent"
    client_cls = AgentClient


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    client = AgentClientManager().get_client("whx-ceph-1")
    res = client.get_host_disks()
    logger.info(res)
