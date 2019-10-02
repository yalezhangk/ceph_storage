#!/usr/bin/env python
# -*- coding: utf-8 -*-
import six
import logging

from stor.agent.tools.base import ToolBase
from stor.exception import RunCommandError

logger = logging.getLogger(__name__)


class Package(ToolBase):
    def install(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")

    def uninstall(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")


class YumPackage(Package):
    def install(self, names, **kwargs):
        logger.debug("Install Package: {}".format(names))
        cmd = ["yum", "install"]
        enable_repos = kwargs.pop("enable_repos", None)
        if enable_repos:
            if isinstance(enable_repos, six.string_types):
                enable_repos = [enable_repos]
            cmd.append("--disablerepo=*")
            cmd.append("--enablerepo={}".format(','.join(enable_repos)))
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def uninstall(self, names):
        logger.debug("Uninstall Package: {}".format(names))
        cmd = ["yum", "uninstall"]
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)
