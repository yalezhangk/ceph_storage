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
        filename = self._wapper(filename)
        self.executor.write(filename, content)

    def mkdir(self, dirname):
        dirname = self._wapper(dirname)
        cmd = ["mkdir", "-p", dirname]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def rm(self, path):
        cmd = ["rm", "-rf", path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def mv(self, src_path, dest_path):
        src_path = self._wapper(src_path)
        dest_path = self._wapper(dest_path)
        cmd = ["mv", "-f", src_path, dest_path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def exist(self, path):
        cmd = ["ls", path]
        rc, stdout, stderr = self.executor.run_command(cmd)
        if not rc:
            return True
        if rc == 2:
            return False
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
