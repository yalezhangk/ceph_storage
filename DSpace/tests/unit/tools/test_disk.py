#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from DSpace import test
from DSpace.tools.base import Executor
from DSpace.tools.disk import DiskTool


class TestDiskTool(test.TestCase):

    @mock.patch.object(Executor, 'run_command')
    @mock.patch("uuid.uuid4",
                return_value='b14030e6-0ce5-11ea-b4e9-000e1eeb6272')
    def test_partations_create(self, uuid4, run_command):
        run_command.return_value = (0, "", "")
        diskname = "sdg"
        tool = DiskTool(Executor())
        tool.partitions_create(
            diskname, [{'name': 'sdg1', 'size': 1998998994944.0, 'role': 'db'}]
        )
        run_command.assert_has_calls([
            mock.call(
                ["sgdisk", "--largest-new=1",
                 "--change-name='1:ceph block.db'",
                 "--partition-guid=1:b14030e6-0ce5-11ea-b4e9-000e1eeb6272",
                 "--typecode=1:30cd0809-c2b2-499c-8879-2d6b78529876",
                 "--mbrtogpt", "--", "/host/dev/sdg"]
            ),
            mock.call(["blkid", "/host/dev/sdg1", "-o", "udev"])
        ])

    @mock.patch.object(Executor, 'run_command')
    def test_partations_clear(self, run_command):
        run_command.return_value = (0, "", "")
        diskname = "sdb"
        tool = DiskTool(Executor())
        tool.partitions_clear(diskname)
        run_command.assert_called_once_with(
            ['wipefs', '/host/dev/sdb', '-a']
        )
