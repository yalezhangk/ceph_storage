#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from DSpace import test
from DSpace.tools.base import Executor
from DSpace.tools.package import YumPackage


class TestServiceTool(test.TestCase):

    @mock.patch.object(Executor, 'run_command')
    def test_package_install_one(self, run_command):
        run_command.return_value = (0, "", "")
        package_name = "test"
        tool = YumPackage(Executor())
        tool.install(package_name)
        run_command.assert_called_once_with(
            ['yum', 'install', '-y', package_name]
        )

    @mock.patch.object(Executor, 'run_command')
    def test_package_install_multiple(self, run_command):
        run_command.return_value = (0, "", "")
        package_names = ['a', 'b']
        tool = YumPackage(Executor())
        tool.install(package_names)
        cmd = ['yum', 'install', '-y']
        cmd.extend(package_names)
        run_command.assert_called_once_with(cmd)

    @mock.patch.object(Executor, 'run_command')
    def test_package_install_enable_repos(self, run_command):
        run_command.return_value = (0, "", "")
        package_names = ['a', 'b']
        enable_repos = ['r1', 'r2']
        tool = YumPackage(Executor())
        tool.install(package_names, enable_repos=enable_repos)
        cmd = ['yum', 'install', '-y']
        cmd.extend([
            '--disablerepo=*',
            '--enablerepo={}'.format(','.join(enable_repos))
        ])
        cmd.extend(package_names)
        run_command.assert_called_once_with(cmd)

    @mock.patch.object(Executor, 'run_command')
    def test_package_uninstall(self, run_command):
        run_command.return_value = (0, "", "")
        package_name = "test"
        tool = YumPackage(Executor())
        tool.uninstall(package_name)
        run_command.assert_called_once_with(
            ['yum', 'remove', '-y', package_name]
        )
