#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from DSpace import test
from DSpace.tools.base import Executor
from DSpace.tools.disk import DiskTool


class TestDiskTool(test.TestCase):

    @mock.patch.object(Executor, 'run_command')
    def test_partations_create(self, run_command):
        run_command.return_value = (0, "", "")
        diskname = "sdb"
        tool = DiskTool(Executor(), host_prefix="/host")
        tool.partitions_create(diskname, ["0%", "20%", "100%"])
        run_command.assert_called_once_with(
            ['parted', '/host/dev/sdb', 'mklabel', 'gpt', 'mkpart', 'primary',
             '0%', '20%', 'mkpart', 'primary', '20%', '100%']
        )

    @mock.patch.object(Executor, 'run_command')
    def test_partations_clear(self, run_command):
        run_command.return_value = (0, "", "")
        diskname = "sdb"
        tool = DiskTool(Executor(), host_prefix="/host")
        tool.partitions_clear(diskname)
        run_command.assert_called_once_with(
            ['wipefs', '/host/dev/sdb', '-a']
        )
