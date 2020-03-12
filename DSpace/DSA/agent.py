from oslo_log import log as logging

from DSpace import exception
from DSpace.DSA.ceph import CephHandler
from DSpace.DSA.cron import CronHandler
from DSpace.DSA.disk import DiskHandler
from DSpace.DSA.iscsi import IscsiHandler
from DSpace.DSA.network import NetworkHandler
from DSpace.DSA.node import NodeHandler
from DSpace.DSA.prometheus import PrometheusHandler
from DSpace.DSA.radosgw import RadosgwHandler
from DSpace.DSA.service import ServiceHandler
from DSpace.DSA.socket_domain import SocketDomainHandler
from DSpace.i18n import _
from DSpace.service import ServiceBase
from DSpace.tools.log_file import LogFile as LogFileTool
from DSpace.tools.service import Service as ServiceTool

logger = logging.getLogger(__name__)


class AgentHandler(CronHandler, CephHandler, DiskHandler, NetworkHandler,
                   IscsiHandler, PrometheusHandler, NodeHandler,
                   ServiceHandler, SocketDomainHandler, RadosgwHandler):

    def service_restart(self, context, name):
        logger.debug("Service restart: %s", name)
        tool = ServiceTool(self.executor)
        tool.restart(name)
        return True

    def check_dsa_status(self, context):
        logger.info("DSA start success")
        return self.state

    def get_logfile_metadata(self, ctxt, node, service_type):
        logger.debug('begin get_logfile_metadata,service_type:%s',
                     service_type)
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            logger.error('connect to node error,node_ip:%s', node.ip_address)
            return False
        log_file_tool = LogFileTool(ssh_client)
        try:
            metadata = log_file_tool.get_logfile_metadata(service_type)
            logger.info("get_logfile_metadata success:{}".format(service_type))
        except exception.StorException as e:
            logger.exception("get_logfile_metadata error:%s", e)
            metadata = None
        return metadata

    def read_log_file_content(self, ctxt, node, directory, filename, offset,
                              length):
        logger.info('begin read_log_file_content, file_name:%s', filename)
        executor = self._get_executor()
        log_file_tool = LogFileTool(executor)
        try:
            content = log_file_tool.read_log_file_content(
                directory, filename, offset, length)
            logger.info('read log_file success, file_name:%s', filename)
        except Exception as e:
            logger.exception('read log_file error:%s', e)
            raise exception.DownloadFileError(reason=_(str(e)))
        return content

    def log_file_size(self, ctxt, node, directory, filename):
        logger.info('begin get log_file_size, file_name:%s', filename)
        executor = self._get_executor()
        log_file_tool = LogFileTool(executor)
        file_size = 0
        try:
            file_size = log_file_tool.log_file_size(directory, filename)
            logger.info('read log_file success, file_name: %s, file_size: %s',
                        filename, file_size)
        except Exception as e:
            logger.exception('read log_file error:%s', e)
            raise exception.GetFileSizeError(reason=_(str(e)))
        return file_size


class AgentService(ServiceBase):
    service_name = "agent"

    def __init__(self, *args, **kwargs):
        self.handler = AgentHandler()
        super(AgentService, self).__init__(*args, **kwargs)
