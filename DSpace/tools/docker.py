#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.exception import ProgrammingError
from DSpace.exception import RunCommandError
from DSpace.objects import fields as s_fields
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class Docker(ToolBase):
    def run(self, name, image, command=None, volumes=None, envs=None,
            privileged=False, restart=True):
        """Run container

        :param name: the container name
        :param image: image
        :param cmd: run cmd
        :param volumes: container volumes
        :param envs: container environments
        """
        logger.debug("Docker run: {}".format(name))
        cmd = ["docker", "run", "-d"]
        cmd.extend(["--name", name])
        cmd.extend(["--network", "host"])
        cmd.extend(["-v", "/etc/localtime:/etc/localtime:ro"])
        volumes = volumes or []
        for volume in volumes:
            if len(volume) == 2:
                cmd.extend(["-v", "{}:{}".format(volume[0], volume[1])])
            elif len(volume) == 3:
                cmd.extend(["-v", "{}:{}:{}".format(
                    volume[0], volume[1], volume[2])])
            else:
                raise ProgrammingError('volumes args error')
        envs = envs or []
        for k, v in envs:
            cmd.extend(["-e", "{}={}".format(k, v)])
        if privileged:
            cmd.extend(["--privileged"])
        if restart:
            cmd.extend(["--restart", "unless-stopped"])
        cmd.append(image)
        if command:
            cmd.append(command)

        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def restart(self, name):
        logger.debug("Docker restart: {}".format(name))
        cmd = ["docker", "restart", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def stop(self, name):
        logger.debug("Docker stop: {}".format(name))
        cmd = ["docker", "stop", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def rm(self, name, force=False):
        logger.debug("Docker rm: {}".format(name))
        cmd = ["docker", "rm", name]
        if force:
            cmd.append("-f")
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def image_rm(self, name, force=False):
        logger.debug("Docker image rm: {}".format(name))
        cmd = ["docker", "image", "rm", name]
        if force:
            cmd.append("-f")
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def volume_rm(self, name, force=False):
        logger.debug("Docker volume rm: {}".format(name))
        cmd = ["docker", "volume", "rm", name]
        if force:
            cmd.append("-f")
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def image_load(self, filename):
        logger.debug("Docker load image: {}".format(filename))
        cmd = ["docker", "load", "-i", filename]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def available(self):
        logger.info("Test docker available")
        cmd = ["docker", "ps"]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            return False
        else:
            return True

    def status(self, name):
        """Show docker container status

        :param name: the container name.
        :returns: status of container.
                  error: not exists
                  inactive: stop
                  active: running
        """
        logger.debug("Docker status: {}".format(name))
        cmd = ["docker", "inspect", "-f", "{{.State.Running}}", name]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            if "No such object" in stderr:
                return "error"
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        if stdout.strip() == "true":
            return s_fields.ServiceStatus.ACTIVE
        else:
            return s_fields.ServiceStatus.INACTIVE

    def exist(self, name):
        logger.info("Check if container is existed: %s", name)
        cmd = ['docker', 'ps', '-a', '|', 'grep', name]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            if not stderr:
                return False
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        if stdout:
            return True
        else:
            return False
