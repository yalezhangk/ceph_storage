#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from os import path

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


def _get_tool_path(name):
    root = path.dirname(path.dirname(__file__))
    return path.join(root, "tools", "remote", "%s" % name)


class ProbeTool(ToolBase):
    def probe_cluster_nodes(self):
        tool = "ceph_collect.py"
        ssh = self.executor
        ssh.write("/tmp/ceph_collect.py", open(_get_tool_path(tool)).read())
        cmd = ['python', "/tmp/ceph_collect.py", "collect_nodes"]
        rc, out, err = ssh.run_command(cmd)
        if not rc:
            logger.info(out)
            return json.loads(out)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=out, stderr=err)

    def probe_node_services(self):
        tool = "ceph_collect.py"
        self.executor.write(
            "/tmp/ceph_collect.py", open(_get_tool_path(tool)).read()
        )
        cmd = ['python', "/tmp/ceph_collect.py", "ceph_services"]
        rc, out, err = self.executor.run_command(cmd)
        if not rc:
            logger.info(out)
            return json.loads(out)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=out, stderr=err)

    def probe_node_osd(self):
        tool = "ceph_collect.py"
        self.executor.write(
            "/tmp/ceph_collect.py", open(_get_tool_path(tool)).read()
        )
        cmd = ['python', "/tmp/ceph_collect.py", "ceph_osd"]
        rc, out, err = self.executor.run_command(cmd)
        if not rc:
            logger.info(out)
            return json.loads(out)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=out, stderr=err)

    def probe_ceph_config(self):
        tool = "ceph_collect.py"
        self.executor.write(
            "/tmp/ceph_collect.py", open(_get_tool_path(tool)).read()
        )
        cmd = ['python', "/tmp/ceph_collect.py", "ceph_config"]
        rc, out, err = self.executor.run_command(cmd)
        if not rc:
            logger.info(out)
            return json.loads(out)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=out, stderr=err)

    def probe_admin_keyring(self):
        tool = "ceph_collect.py"
        self.executor.write(
            "/tmp/ceph_collect.py", open(_get_tool_path(tool)).read()
        )
        cmd = ['python', "/tmp/ceph_collect.py", "ceph_keyring"]
        rc, out, err = self.executor.run_command(cmd)
        if not rc:
            logger.info(out)
            return json.loads(out)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=out, stderr=err)
