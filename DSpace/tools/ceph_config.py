#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import logging

from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class CephConfigTool(ToolBase):
    def __init__(self, path, *args, **kwargs):
        super(CephConfigTool, self).__init__(*args, **kwargs)
        self.cfg = configparser.ConfigParser()
        if path:
            self.path = self._wapper(path)
            self.cfg.read(self.path)

    def set_value(self, key, value, section='global'):
        if section not in self.cfg:
            self.cfg[section] = {}
        self.cfg[section][key] = value

    def clear_key(self, key, section='global'):
        self.cfg.remove_option(section, key)

    def set_section(self, section, configs):
        self.cfg[section] = configs

    def clear_section(self, section):
        self.cfg.remove_section(section)

    def save(self):
        logger.info("write to ceph config: %s", self.path)
        with open(self.path, 'w') as configfile:
            self.cfg.write(configfile)


def test():
    from DSpace.common.config import CONF
    CONF.host_prefix = ''
    from DSpace.tools.base import Executor
    cfg = CephConfigTool("/etc/ceph/ceph.conf.b", Executor())
    cfg.set_value('abc', 'abc')
    cfg.set_value('abc', 'abc', 'abc')
    cfg.set_value('clear', 'abc')
    cfg.clear_key('clear')
    cfg.set_value('clear', 'abc', 'clear')
    cfg.clear_key('clear', 'clear')
    cfg.set_section('section', {'a': "b"})
    cfg.set_section('clear_section', {'a': "b"})
    cfg.clear_section('clear_section')
    cfg.save()


if __name__ == '__main__':
    test()
