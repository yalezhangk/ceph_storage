#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
from io import StringIO

from t2stor import objects


class CephTask(object):
    ctxt = None

    def __init__(self, ctxt):
        self.ctxt = ctxt

    def ceph_config(self):
        configer = configparser.ConfigParser()
        configs = objects.CephConfigList.get_all(self.ctxt)
        for config in configs:
            if not configer.has_section(config.group):
                configer[config.group] = {}
            configer[config.group][config.key] = config.value
        buf = StringIO()
        configer.write(buf)
        return buf.getvalue()

    def pool_create(self):
        pass

    def pool_add_osd(self):
        pass

    def pool_rm_osd(self):
        pass

    def pool_rm(self):
        pass
