#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from t2stor.tools.base import ToolBase

logger = logging.getLogger(__name__)


class File(ToolBase):
    def write_from(self, filename, url):
        """Create file from url"""
        raise NotImplementedError("Method Not ImplementedError")

    def write(self, filename, content):
        self.executor.write(filename, content)

    def mkdir(self, dirname):
        self.executor.run_command(["mkdir", "-p", dirname])
