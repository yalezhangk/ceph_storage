from concurrent import futures
import sys
import time
import logging
import json

import grpc

from . import agent_pb2
from . import agent_pb2_grpc
from ..service import ServiceBase

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


example = {
    "disks": [{
        "name": "vda",
        "size": 3 * 1024**3,
    },{
        "name": "vdb",
        "size": 5 * 1024**3,
    }]
}


class Agent(agent_pb2_grpc.AgentServicer):

    def GetDiskInfo(self, request, context):
        logger.debug("get disk info")
        return agent_pb2.Response(value=json.dumps(example))

    def WriteCephConf(self, request, context):
        logger.debug("Write Ceph Conf")
        return agent_pb2.Response(value="Success")

    def InstallPackage(self, request, context):
        logger.debug("Install Package")
        return agent_pb2.Response(value="Success")

    def StartService(self, request, context):
        logger.debug("Install Package")
        time.sleep(1)
        logger.debug("Start Service")
        time.sleep(1)
        return agent_pb2.Response(value="Success")


class AgentService(ServiceBase):
    service_name = "agent"
    rpc_endpoint = None
    rpc_ip = "172.159.4.11"
    rpc_port = 50052

    def __init__(self, hostname=None, ip=None):
        super(AgentService, self).__init__()
        self.hostname = hostname
        if ip:
            self.rpc_ip = ip
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })

    def rpc_register_service(self, server):
        agent_pb2_grpc.add_AgentServicer_to_server(Agent(), server)


if __name__ == '__main__':
    logging.basicConfig(
        level="DEBUG",
        format='%(asctime)s :: %(levelname)s :: %(message)s'
    )
    if len(sys.argv) < 3:
        print("%s hostname ip" % sys.argv[0])
        exit(0)
    AgentService(hostname=sys.argv[1], ip=sys.argv[2]).run()


