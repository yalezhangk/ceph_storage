from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase


class RadosgwTool(ToolBase):

    def __init__(self, *args, **kwargs):
        super(RadosgwTool, self).__init__(*args, **kwargs)

    def _run_command(self, cmd):
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return stdout

    def get_rgw_gateway_cup_and_memory(self, rgw_names):
        datas = []
        for name in rgw_names:
            data = {'ceph_daemon': name}
            ps_cmd = ["ps", "-ef", "|", "grep", name, "|", "grep", "-v",
                      "grep", "|", "awk", "'{print $2}'"]
            ps_out = self._run_command(ps_cmd)
            rgw_pid = ps_out.strip('\n')
            cpu_out = self._run_command(['pidstat', '-p', rgw_pid, '-u'])
            cpu_percent = cpu_out.strip('\n').split('   ')[-2].strip()
            memory_out = self._run_command(['pidstat', '-p', rgw_pid, '-r'])
            memory_percent = memory_out.strip('\n').split('  ')[-2].strip()
            data.update({'cpu_percent': float(cpu_percent),
                         'memory_percent': float(memory_percent)})
            datas.append(data)
        return datas
