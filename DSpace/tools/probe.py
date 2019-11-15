#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from os import path

import paramiko

from DSpace.exception import RunCommandError
from DSpace.exception import SSHInvalid
from DSpace.tools.base import SSHExecutor

logger = logging.getLogger(__name__)


def _get_tool_path(name):
    root = path.dirname(path.dirname(__file__))
    return path.join(root, "tools", "remote", "%s" % name)


def probe_cluster_nodes(ip, password, user='root', port=''):
    try:
        ssh = SSHExecutor(hostname=ip, password=password, user=user, port=port)
    except paramiko.ssh_exception.AuthenticationException as e:
        if "Authentication failed" in str(e):
            raise SSHInvalid(ip=ip, password=password)
        else:
            raise e

    tool = "ceph_collect.py"
    ssh.write("/tmp/ceph_collect.py", open(_get_tool_path(tool)).read())
    cmd = ['python', "/tmp/ceph_collect.py"]
    rc, out, err = ssh.run_command(cmd)
    if not rc:
        logger.info(out)
        return json.loads(out)
    raise RunCommandError(cmd=cmd, return_code=rc,
                          stdout=out, stderr=err)
