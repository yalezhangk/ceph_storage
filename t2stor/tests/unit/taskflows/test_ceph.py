#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mock

from t2stor import objects
from t2stor import test
from t2stor.taskflows.ceph import CephTask


class TestCephTask(test.TestCase):
    @staticmethod
    def _ceph_config(ctxt):
        list_obj = objects.CephConfigList(ctxt)
        list_obj.objects = []
        for c in config_data:
            item = objects.CephConfig(ctxt, **c)
            list_obj.objects.append(item)
        list_obj._context = ctxt
        list_obj.obj_reset_changes()
        return list_obj

    @mock.patch.object(objects.CephConfigList, 'get_all')
    def test_ceph_config(self, get_all):
        ctxt = None
        get_all.return_value = self._ceph_config(ctxt)
        task = CephTask(ctxt)
        config_str = task.ceph_config()
        self.assertEqual(config_content, config_str)


config_data = [{
    "group": "global",
    "key": "mon_initial_members",
    "value": "p71,p72,p74",
}, {
    "group": "global",
    "key": "public_network",
    "value": "172.160.6.0/16",
}, {
    "group": "osd.2",
    "key": "backend_type",
    "value": "kernel",
}, {
    "group": "osd.1",
    "key": "backend_type",
    "value": "t2ce",
}]

config_content = """[global]
mon_initial_members = p71,p72,p74
public_network = 172.160.6.0/16

[osd.2]
backend_type = kernel

[osd.1]
backend_type = t2ce

"""
