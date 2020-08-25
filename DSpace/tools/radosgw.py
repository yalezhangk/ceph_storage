import logging

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class RadosgwTool(ToolBase):

    def __init__(self, *args, **kwargs):
        super(RadosgwTool, self).__init__(*args, **kwargs)

    def _run_command(self, cmd):
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return stdout

    def get_rgw_gateway_cpu_and_memory(self, rgw_names):
        datas = []
        for name in rgw_names:
            try:
                data = self.get_per_gateway_cpu_and_memory(name)
            except Exception as e:
                logger.error('get gateway: %s cpu_memory metrics error: %s'
                             % (name, str(e)))
                continue
            logger.debug('gateway: %s cpu_memory metrics is: %s'
                         % (name, data))
            datas.append(data)
        return datas

    def get_per_gateway_cpu_and_memory(self, name):
        data = {'ceph_daemon': name}
        ps_cmd = ["ps", "-eo", "pmem,pcpu,pid,user,command", "|", "grep", name,
                  "|", "grep", "-v", "grep", "|", "awk", "'{print $1,$2}'"]
        ps_out = self._run_command(ps_cmd)
        ps_list = ps_out.strip('\r\n').split(' ')
        memory_out, cpu_out = ps_list[0], ps_list[1]
        data.update({'cpu_percent': float(cpu_out),
                     'memory_percent': float(memory_out)})
        return data
