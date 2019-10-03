import sys
import time
import json

from oslo_log import log as logging

from stor.service import ServiceBase
from stor import version
from stor.agent.tools.base import Executor
from stor.agent.tools.service import Service
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

    def _get_executor(self):
        return Executor()

    @property
    def executor(self):
        return self._get_executor()

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

    def service_restart(self, context,  name):
        logger.debug("Service restart: %s", name)
        tool = Service(self.executor)
        tool.restart(name)
        return True


class AgentService(ServiceBase):
    service_name = "agent"

    def __init__(self):
        self.handler = AgentHandler()
        super(AgentService, self).__init__()


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
    agent = AgentService()
    agent.start()
    run_loop()
    agent.stop()
