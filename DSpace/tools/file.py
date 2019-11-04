#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class File(ToolBase):
    def write_from(self, filename, url):
        """Create file from url"""
        raise NotImplementedError("Method Not ImplementedError")

    def write(self, filename, content):
        self.executor.write(filename, content)

    def mkdir(self, dirname):
        self.executor.run_command(["mkdir", "-p", dirname])

    def chown(self, path, user='root', group='root'):
        self.executor.run_command(["chown",
                                   "-R",
                                   "{}:{}".format(user, group),
                                   path])

    def chmod(self, path, mode='0644'):
        self.executor.run_command(["chmod", "-R", mode, path])
