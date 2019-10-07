#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from t2stor.tools.docker import Docker
from t2stor.tools.base import Executor
from t2stor.tools.base import ToolBase
from t2stor import test


class TestServiceTool(test.TestCase):

    @mock.patch.object(Executor, 'run_command')
    def test_docker_run(self, run_command):
        run_command.return_value = (0, "", "")
        tool = Docker(Executor())
        tool.run(
            name="container",
            image="image",
            command="cmd",
            volumes=[("a", "/a"), ("b", "/b", "ro")],
            envs=[("ENV1", "value1"), ("ENV2", "value2")]
        )
        run_command.assert_called_once_with([
            'docker', 'run', '-d', '--name', 'container', '--network', 'host',
            '-v', '/etc/localtime:/etc/localtime:ro', '-v', 'a:/a',
            '-v', 'b:/b:ro', '-e', 'ENV1=value1', '-e', 'ENV2=value2',
            'image', 'cmd'
        ])

    @mock.patch.object(Executor, 'run_command')
    def test_docker_stop(self, run_command):
        run_command.return_value = (0, "", "")
        container_name = "test"
        tool = Docker(Executor())
        tool.stop(container_name)
        cmd = ['docker', 'stop', container_name]
        run_command.assert_called_once_with(cmd)

    @mock.patch.object(Executor, 'run_command')
    def test_docker_rm(self, run_command):
        run_command.return_value = (0, "", "")
        container_name = "test"
        tool = Docker(Executor())
        tool.rm(container_name, force=True)
        cmd = ['docker', 'rm', container_name, "-f"]
        run_command.assert_called_once_with(cmd)

    @mock.patch.object(Executor, 'run_command')
    def test_docker_image_rm(self, run_command):
        run_command.return_value = (0, "", "")
        container_name = "test"
        tool = Docker(Executor())
        tool.image_rm(container_name, force=True)
        cmd = ['docker', 'image', 'rm', container_name, "-f"]
        run_command.assert_called_once_with(cmd)

    @mock.patch.object(Executor, 'run_command')
    def test_docker_volume_rm(self, run_command):
        run_command.return_value = (0, "", "")
        container_name = "test"
        tool = Docker(Executor())
        tool.volume_rm(container_name, force=True)
        cmd = ['docker', 'volume', 'rm', container_name, "-f"]
        run_command.assert_called_once_with(cmd)
