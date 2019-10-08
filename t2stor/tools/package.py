#!/usr/bin/env python
# -*- coding: utf-8 -*-
import six
import logging

from t2stor.tools.base import ToolBase
from t2stor.exception import RunCommandError

logger = logging.getLogger(__name__)


class PackageBase(ToolBase):
    def install(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")

    def uninstall(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")


class YumPackage(PackageBase):
    def install(self, names, **kwargs):
        logger.debug("Install Package: {}".format(names))
        cmd = ["yum", "install", "-y"]
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
        cmd = ["yum", "remove", '-y']
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)


class Package(ToolBase):
    def __init__(self, executor, *args, **kwargs):
        super(Package, self).__init__(executor, *args, **kwargs)
        # TODO: select yum or apt
        self.tool = YumPackage(executor)

    def install(self, names, **kwargs):
        self.tool.install(names=names, **kwargs)

    def uninstall(self, names, **kwargs):
        self.tool.uninstall(names=names, **kwargs)
