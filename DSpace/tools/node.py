#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os

from oslo_log import log as logging

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.tools.utils import get_file_content

logger = logging.getLogger(__name__)


class NodeTool(ToolBase):

    def __init__(self, *args, **kwargs):
        super(NodeTool, self).__init__(*args, **kwargs)

    def parse_cpu_info(self, cpu_info):
        for line in cpu_info.split('\n'):
            if not line:
                continue
            words = line.split(':')
            if words[0] == 'Socket(s)':
                cpu_num = words[1].strip()
            elif words[0] == 'Model name':
                cpu_model = words[1].strip()
            elif words[0] == 'CPU(s)':
                cpu_core_num = words[1].strip()
        return cpu_num, cpu_model, cpu_core_num

    def parse_meminfo(self, mem_info):
        for line in mem_info.split('\n'):
            if not line:
                continue
            words = line.split(':')
            if words[0] == "MemTotal":
                memsize = int(words[1].strip().split(" ")[0])
        return memsize / 1024

    def parse_os_release(self, os_info):
        sys_type = None
        for line in os_info.split('\n'):
            if not line:
                continue
            words = line.split('=')
            if words[0] == "PRETTY_NAME":
                sys_type = words[1].strip('"')
        return sys_type

    def parse_os_version(self, os_info):
        sys_version = None
        for line in os_info.split('\n'):
            if not line:
                continue
            words = line.split(' ')
            sys_version = words[2]
        return sys_version

    def summary(self):
        sys_path = self._wapper('/sys/devices/virtual/dmi/id')
        logger.debug("Gather node summary from %s", sys_path)
        node_summary = {}
        vendor = None
        if os.path.exists(os.path.join(sys_path, 'product_name')):
            vendor = get_file_content(os.path.join(sys_path, 'product_name'))

        args = ['cat', '/proc/meminfo']
        rc, data, stderr = self.executor.run_command(args)
        if rc:
            raise RunCommandError(cmd=args, return_code=rc,
                                  stdout=data, stderr=stderr)
        memsize = self.parse_meminfo(data)

        os_release_file = self._wapper('/etc/os-release')
        logger.info("get os release: %s", os_release_file)
        args = ['cat', os_release_file]
        rc, data, stderr = self.executor.run_command(args)
        if rc:
            raise RunCommandError(cmd=args, return_code=rc,
                                  stdout=data, stderr=stderr)
        sys_type = self.parse_os_release(data)

        args = ['cat', '/proc/version']
        rc, data, stderr = self.executor.run_command(args)
        if rc:
            raise RunCommandError(cmd=args, return_code=rc,
                                  stdout=data, stderr=stderr)
        sys_version = self.parse_os_version(data)

        args = ['lscpu']
        rc, data, stderr = self.executor.run_command(args)
        if rc:
            raise RunCommandError(cmd=args, return_code=rc,
                                  stdout=data, stderr=stderr)
        cpu_num, cpu_model, cpu_core_num = self.parse_cpu_info(data)

        cmd = ['hostname']
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        else:
            hostname = stdout.strip()
        node_summary = {
            "hostname": hostname,
            "sys_type": sys_type,
            "sys_version": sys_version,
            "vendor": vendor,
            "memsize": memsize,
            "cpu_num": cpu_num,
            "cpu_model": cpu_model,
            "cpu_core_num": cpu_core_num
        }
        return node_summary


if __name__ == '__main__':
    from DSpace.tools.base import Executor
    from DSpace.common.config import CONF
    logging.setup(CONF, "stor")
    t = NodeTool(Executor(), host_prefix="")
    print(json.dumps(t.summary()))
