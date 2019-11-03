import sys
import time

from oslo_log import log as logging

from t2stor import exception
from t2stor import version
from t2stor.agent.ceph import CephHandler
from t2stor.agent.cron import CronHandler
from t2stor.agent.disk import DiskHandler
from t2stor.common.config import CONF
from t2stor.service import ServiceBase
from t2stor.tools.log_file import LogFile as LogFileTool
from t2stor.tools.service import Service as ServiceTool

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AgentHandler(CronHandler, CephHandler, DiskHandler):

    def service_restart(self, context, name):
        logger.debug("Service restart: %s", name)
        tool = ServiceTool(self.executor)
        tool.restart(name)
        return True

    def get_logfile_metadata(self, ctxt, node, service_type):
        logger.debug('begin get_logfile_metadata,service_type:%s',
                     service_type)
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            return False
        log_file_tool = LogFileTool(ssh_client)
        try:
            metadata = log_file_tool.get_logfile_metadata(service_type)
            logger.info("get_logfile_metadata success:{}".format(service_type))
        except exception.StorException as e:
            logger.error("get_logfile_metadata error:{}".format(e))
            metadata = None
        return metadata


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


def service():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    agent = AgentService()
    agent.start()
    run_loop()
    agent.stop()
