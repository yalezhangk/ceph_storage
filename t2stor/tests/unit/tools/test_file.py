#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock

from t2stor.tools.file import File
from t2stor.tools.base import Executor
from t2stor import test


class TestFileTool(test.TestCase):

    @mock.patch.object(Executor, 'write')
    def test_file_write(self, run_command):
        run_command.return_value = (True)
        tool = File(Executor())
        tool.write("a", "content")
        run_command.assert_called_once_with("a", "content")

    @mock.patch.object(Executor, 'run_command')
    def test_mkdir(self, run_command):
        run_command.return_value = (0, "", "")
        dirname = "test"
        tool = File(Executor())
        tool.mkdir(dirname)
        run_command.assert_called_once_with(
            ['mkdir', '-p', dirname]
        )
