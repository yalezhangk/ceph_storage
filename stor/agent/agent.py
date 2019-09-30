import sys
import time
import json

from oslo_log import log as logging

from stor.service import ServiceBase
from stor import version
from stor.common.config import CONF


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


example = {
    "disks": [{
        "name": "vda",
        "size": 3 * 1024**3,
    }, {
        "name": "vdb",
        "size": 5 * 1024**3,
    }]
}


class AgentHandler(object):

    def disk_get_all(self, context):
        logger.debug("disk get all")
        return []

    def ceph_conf_write(self, context, conf):
        logger.debug("Write Ceph Conf")
        return True

    def package_install(self, context, packages):
        logger.debug("Install Package")
        return True

    def service_start(self, context, service_info):
        logger.debug("Install Package")
        time.sleep(1)
        logger.debug("Start Service")
        time.sleep(1)
        return True


class AgentService(ServiceBase):
    service_name = "agent"
    rpc_endpoint = None
    rpc_ip = "192.168.211.129"
    rpc_port = 2082

    def __init__(self, hostname=None, ip=None):
        self.handler = AgentHandler()
        if ip:
            self.rpc_ip = ip
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })
        super(AgentService, self).__init__(hostname=hostname)


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    AgentService().start()
    run_loop()
