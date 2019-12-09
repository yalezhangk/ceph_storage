import logging
from pathlib import Path

import paramiko

from DSpace.tools.base import SSHExecutor
from DSpace.tools.probe import ProbeTool

logger = logging.getLogger(__name__)


class ProbeTask(object):
    ctxt = None
    node = None
    host_perfix = None

    def __init__(self, ctxt, node, host_prefix=None):
        self.host_prefix = host_prefix
        self.ctxt = ctxt
        self.node = node

    def get_ssh_executor(self):
        return SSHExecutor(hostname=str(self.node.ip_address),
                           password=self.node.password)

    def get_ssh_key(self):
        home = str(Path.home())
        pk = paramiko.RSAKey.from_private_key(open('%s/.ssh/id_rsa' % home))
        return pk

    def probe_cluster_nodes(self):
        ssh = self.get_ssh_executor()
        probe_tool = ProbeTool(ssh)
        result = probe_tool.probe_cluster_nodes()
        return result

    def check_planning(self):
        ssh = self.get_ssh_executor()
        probe_tool = ProbeTool(ssh)
        result = probe_tool.check_planning()
        return result
