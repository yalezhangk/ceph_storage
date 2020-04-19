#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

import docker

from DSpace.exception import ActionTimeoutError
from DSpace.exception import DockerSockCmdError
from DSpace.exception import DockerSockNotFound
from DSpace.exception import ProgrammingError
from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.utils import retry

logger = logging.getLogger(__name__)


class Docker(ToolBase):
    def run(self, name, image, command=None, volumes=None, envs=None,
            privileged=False, restart=True, registry=None):
        """Run container

        :param name: the container name
        :param image: image
        :param cmd: run cmd
        :param volumes: container volumes
        :param envs: container environments
        """
        logger.debug("Docker run: {}".format(name))
        if registry:
            image = "{}/{}".format(registry, image)
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
            cmd.extend(["--restart", "on-failure:5"])
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
        if "No such container" in stderr:
            return False
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

    @retry(ActionTimeoutError, retries=7)
    def wait_available(self):
        r = self.available()
        if not r:
            logger.info("Docker service not available")
            raise ActionTimeoutError("Docker service not available")

    def status(self, name):
        """Show docker container status

        :param name: the container name.
        :returns: status of container.
                  error: not exists
                  inactive: stop
                  active: running
        """
        logger.debug("Docker status: {}".format(name))
        cmd = ["docker", "inspect", "-f", "{{.State.Status}}", name]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            if "No such object" in stderr:
                return False
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        if stdout.strip() == "running":
            return True
        else:
            return False

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


class DockerSocket(ToolBase):
    SOCKET_FILE = "/var/run/docker.sock"

    def __init__(self, *args, **kwargs):
        super(DockerSocket, self).__init__(*args, **kwargs)
        self.docker_sock = None
        self._check_socket_file()
        self.client = docker.DockerClient(base_url=self.docker_sock)

    def _check_socket_file(self):
        docker_socket = self._wapper(self.SOCKET_FILE)
        if not os.path.exists(docker_socket):
            raise DockerSockNotFound(path=docker_socket)
        self.docker_sock = "unix://" + docker_socket

    def status(self, container_name):
        """
        :param container_name: The name of container
        :return:  Return the status of container. Status will be 'running'，
                 'restarting' or 'exited'. If errors happen, it will return
                 None.
        """
        logger.debug("Get container status for %s", container_name)
        try:
            container = self.client.containers.get(container_name)
            status = container.status
        except Exception as e:
            logger.warning(e)
            status = None
        return status

    def restart(self, container_name):
        logger.debug("Restart container for %s", container_name)
        try:
            container = self.client.containers.get(container_name)
            container.restart()
        except docker.errors.APIError as e:
            logger.warning(e)
            raise DockerSockCmdError(cmd="restart", reason=str(e))

    def get_sys_memory_total(self):
        # return sys_memory_total_bytes, type:int
        m_path = self._wapper('/proc/meminfo')
        cmd = ['cat', m_path]
        result = self.run_command(cmd)
        memory_kb = int(result[1].split('\n')[0].split()[1].strip())
        return memory_kb

    def stats(self, container_name):
        """
        Stream statistics for this container. Similar to the
        ``docker stats`` command.

        Args:
            decode (bool): If set to true, stream will be decoded into dicts
                on the fly. Only applicable if ``stream`` is True.
                False by default.
            stream (bool): If set to false, only the current stats will be
                returned instead of a stream. True by default.
        return: CPU、Memory
        """
        logger.debug("Get container stats for %s", container_name)
        try:
            container = self.client.containers.get(container_name)
            result = container.stats(stream=False)
        except docker.errors.APIError as e:
            logger.warning(e)
            raise DockerSockCmdError(cmd="stats", reason=str(e))
        # CPU
        cpu_usage = result['cpu_stats']['cpu_usage']['total_usage']
        precpu_usage = result['precpu_stats']['cpu_usage']['total_usage']
        cpu_usage = cpu_usage - precpu_usage
        system_cpu = result['cpu_stats']['system_cpu_usage']
        presystem_cpu = result['precpu_stats']['system_cpu_usage']
        system_cpu = system_cpu - presystem_cpu
        cpu_usage_rate_percent = (cpu_usage/system_cpu) * 100
        # Memory
        memory_stats = result['memory_stats']
        memory_used = memory_stats['usage']
        active_file = memory_stats['stats']['active_file']
        inactive_file = memory_stats['stats']['inactive_file']
        memory_used_bytes = memory_used - active_file - inactive_file
        sys_memory_kb = self.get_sys_memory_total()
        memory_rate_percent = (memory_used_bytes/(sys_memory_kb * 1024)) * 100
        return {
            'cpu_usage': cpu_usage,
            'system_cpu': system_cpu,
            'cpu_usage_rate_percent': '{:.2f}'.format(cpu_usage_rate_percent),
            'memory_used_bytes': memory_used_bytes,
            'sys_memory_kb': sys_memory_kb,
            'memory_rate_percent': '{:.2f}'.format(memory_rate_percent),
            'container_name': container_name
        }
