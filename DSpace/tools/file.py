#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class File(ToolBase):
    def fetch_from_url(self, filename, url):
        """fetch file from url"""
        cmd = ["curl", "-o", filename, url]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def write(self, filename, content):
        self.executor.write(filename, content)

    def mkdir(self, dirname):
        cmd = ["mkdir", "-p", dirname]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def rm(self, path):
        cmd = ["rm", "-f", path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def chown(self, path, user='root', group='root'):
        cmd = ["chown", "-R", "{}:{}".format(user, group), path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def chmod(self, path, mode='0644'):
        cmd = ["chmod", "-R", mode, path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)
