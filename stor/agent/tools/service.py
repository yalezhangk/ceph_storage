#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from stor.agent.tools.base import ToolBase
from stor.exception import RunCommandError

logger = logging.getLogger(__name__)


class Service(ToolBase):
    def enable(self, name):
        logger.debug("Service Enable: {}".format(name))
        cmd = ["systemctl", "enable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def start(self, name):
        logger.debug("Service Start: {}".format(name))
        cmd = ["systemctl", "start", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def stop(self, name):
        logger.debug("Service Stop: {}".format(name))
        cmd = ["systemctl", "stop", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def disable(self, name):
        logger.debug("Service Disable: {}".format(name))
        cmd = ["systemctl", "disable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)
