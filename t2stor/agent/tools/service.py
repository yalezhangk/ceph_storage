#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from t2stor.agent.tools.base import ToolBase
from t2stor.exception import RunCommandError

logger = logging.getLogger(__name__)


class Service(ToolBase):
    def enable(self, name):
        logger.debug("Service enable: {}".format(name))
        cmd = ["systemctl", "enable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def start(self, name):
        logger.debug("Service start: {}".format(name))
        cmd = ["systemctl", "start", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def stop(self, name):
        logger.debug("Service stop: {}".format(name))
        cmd = ["systemctl", "stop", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def disable(self, name):
        logger.debug("Service disable: {}".format(name))
        cmd = ["systemctl", "disable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def restart(self, name):
        logger.debug("Service restart: {}".format(name))
        cmd = ["systemctl", "restart", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)
