#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from DSpace import test
from DSpace.tools.base import Executor
from DSpace.tools.file import File


class TestFileTool(test.TestCase):

    @mock.patch.object(Executor, 'write')
    def test_file_write(self, run_command):
        run_command.return_value = (True)
        tool = File(Executor(host_prefix="/a"))
        tool.write("a", "content")
        run_command.assert_called_once_with("/a/a", "content")

    @mock.patch.object(Executor, 'run_command')
    def test_mkdir(self, run_command):
        run_command.return_value = (0, "", "")
        dirname = "test"
        tool = File(Executor())
        tool.mkdir(dirname)
        run_command.assert_called_once_with(
            ['mkdir', '-p', dirname]
        )
