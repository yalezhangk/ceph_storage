#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from t2stor import test
from t2stor.tools.base import Executor
from t2stor.tools.service import Service


class TestServiceTool(test.TestCase):

    @mock.patch.object(Executor, 'run_command')
    def test_service_start(self, run_command):
        run_command.return_value = (0, "", "")
        service_name = "test"
        tool = Service(Executor())
        tool.start(service_name)
        run_command.assert_called_once_with(
            ['systemctl', 'start', service_name]
        )

    @mock.patch.object(Executor, 'run_command')
    def test_service_stop(self, run_command):
        run_command.return_value = (0, "", "")
        service_name = "test"
        tool = Service(Executor())
        tool.stop(service_name)
        run_command.assert_called_once_with(
            ['systemctl', 'stop', service_name]
        )

    @mock.patch.object(Executor, 'run_command')
    def test_service_enable(self, run_command):
        run_command.return_value = (0, "", "")
        service_name = "test"
        tool = Service(Executor())
        tool.enable(service_name)
        run_command.assert_called_once_with(
            ['systemctl', 'enable', service_name]
        )

    @mock.patch.object(Executor, 'run_command')
    def test_service_disable(self, run_command):
        run_command.return_value = (0, "", "")
        service_name = "test"
        tool = Service(Executor())
        tool.disable(service_name)
        run_command.assert_called_once_with(
            ['systemctl', 'disable', service_name]
        )
