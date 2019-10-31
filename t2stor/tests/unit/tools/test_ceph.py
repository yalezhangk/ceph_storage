#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import mock

from t2stor import test
from t2stor.tools.base import Executor
from t2stor.tools.ceph import CephTool
from t2stor.tools.ceph import RADOSClient

get_mons_re = """
{
    "epoch": 1,
    "fsid": "21d56e1c-72f7-4370-8ee6-30edabd7bfd9",
    "modified": "2019-09-30 15:18:12.000027",
    "created": "2019-09-30 15:18:12.000027",
    "features": {
        "persistent": [
            "kraken",
            "luminous"
        ],
        "optional": []
    },
    "mons": [
        {
            "rank": 0,
            "name": "p71",
            "addr": "172.160.6.71:6789/0",
            "public_addr": "172.160.6.71:6789/0"
        }
    ],
    "quorum": [
        0
    ]
}
"""
get_mgrs_re = """
{
    "epoch": 7,
    "active_gid": 4109,
    "active_name": "p71",
    "active_addr": "172.160.6.71:6800/50175",
    "available": true,
    "standbys": [],
    "modules": [
        "balancer",
        "prometheus",
        "restful",
        "status"
    ],
    "available_modules": [
        "balancer",
        "dashboard",
        "influx",
        "localpool",
        "prometheus",
        "restful",
        "selftest",
        "status",
        "zabbix"
    ],
    "services": {
        "prometheus": "http://p71:9283/"
    }
}
"""
get_osds_re = """
{
    "epoch": 1694,
    "fsid": "21d56e1c-72f7-4370-8ee6-30edabd7bfd9",
    "created": "2019-09-30 15:18:13.973557",
    "modified": "2019-09-30 19:01:08.638669",
    "flags": "sortbitwise,recovery_deletes,purged_snapdirs",
    "crush_version": 1091,
    "full_ratio": 0.950000,
    "backfillfull_ratio": 0.900000,
    "nearfull_ratio": 0.850000,
    "cluster_snapshot": "",
    "pool_max": 180,
    "max_osd": 36,
    "require_min_compat_client": "jewel",
    "min_compat_client": "jewel",
    "require_osd_release": "luminous",
    "pools": [
    ],
    "osds": [
        {
            "osd": 0,
            "uuid": "6a2f9027-4fff-47e1-9e3d-036e0f4d96b0",
            "up": 0,
            "in": 0,
            "weight": 0.000000,
            "primary_affinity": 1.000000,
            "last_clean_begin": 0,
            "last_clean_end": 0,
            "up_from": 0,
            "up_thru": 0,
            "down_at": 0,
            "lost_at": 0,
            "public_addr": "-",
            "cluster_addr": "-",
            "heartbeat_back_addr": "-",
            "heartbeat_front_addr": "-",
            "state": [
                "exists",
                "new"
            ]
        },
        {
            "osd": 33,
            "uuid": "dbcecbba-5fe8-4f18-87c9-ec2b7319fe30",
            "up": 1,
            "in": 1,
            "weight": 1.000000,
            "primary_affinity": 1.000000,
            "last_clean_begin": 0,
            "last_clean_end": 0,
            "up_from": 1232,
            "up_thru": 1501,
            "down_at": 0,
            "lost_at": 0,
            "public_addr": "172.160.6.69:6806/2754282",
            "cluster_addr": "172.159.3.69:6806/2754282",
            "heartbeat_back_addr": "172.159.3.69:6807/2754282",
            "heartbeat_front_addr": "172.160.6.69:6807/2754282",
            "state": [
                "exists",
                "up"
            ]
        },
        {
            "osd": 34,
            "uuid": "98754443-2d06-40de-a206-cc0b6da44cb7",
            "up": 1,
            "in": 1,
            "weight": 1.000000,
            "primary_affinity": 1.000000,
            "last_clean_begin": 0,
            "last_clean_end": 0,
            "up_from": 1229,
            "up_thru": 1499,
            "down_at": 0,
            "lost_at": 0,
            "public_addr": "172.160.6.70:6802/1731091",
            "cluster_addr": "172.159.3.70:6802/1731091",
            "heartbeat_back_addr": "172.159.3.70:6803/1731091",
            "heartbeat_front_addr": "172.160.6.70:6803/1731091",
            "state": [
                "exists",
                "up"
            ]
        }
    ],
    "pg_upmap": [],
    "pg_upmap_items": [],
    "pg_temp": [],
    "primary_temp": [],
    "blacklist": {},
    "erasure_code_profiles": {
        "default": {
            "k": "2",
            "m": "1",
            "plugin": "jerasure",
            "technique": "reed_sol_van"
        }
    }
}
"""


class TestServiceTool(test.TestCase):
    @mock.patch.object(Executor, 'run_command')
    def test_get_networks(self, run_command):
        run_command.return_value = (0, "172.159.3.0/16", "")
        tool = CephTool(Executor())
        re = tool.get_networks()
        self.assertEqual(("172.159.3.0/16", "172.159.3.0/16"), re)
        run_command.assert_has_calls([
            mock.call("ceph-conf --lookup cluster_network", timeout=1),
            mock.call("ceph-conf --lookup public_network", timeout=1)
        ])


class TestRadosClient(test.TestCase):

    @mock.patch('t2stor.tools.ceph.rados')
    def test_get_mon_hosts(self, rados):
        rados.Rados = mock.MagicMock()
        rados.Rados().mon_command.return_value = (0, get_mons_re, "")
        tool = RADOSClient("config")
        re = tool.get_mon_hosts()
        self.assertEqual(['172.160.6.71'], re)
        cmd = {"prefix": "mon dump", "format": "json"}
        rados.Rados().mon_command.assert_called_once_with(
            json.dumps(cmd), '')

    @mock.patch('t2stor.tools.ceph.rados')
    def test_get_osd_hosts(self, rados):
        rados.Rados = mock.MagicMock()
        rados.Rados().mon_command.return_value = (0, get_osds_re, "")
        tool = RADOSClient("config")
        re = tool.get_osd_hosts()
        self.assertEqual(sorted(['172.160.6.69', '172.160.6.70']), sorted(re))
        cmd = {"prefix": "osd dump", "format": "json"}
        rados.Rados().mon_command.assert_called_once_with(
            json.dumps(cmd), '')

    @mock.patch('t2stor.tools.ceph.rados')
    def test_get_mgr_hosts(self, rados):
        rados.Rados = mock.MagicMock()
        rados.Rados().mon_command.return_value = (0, get_mgrs_re, "")
        tool = RADOSClient("config")
        re = tool.get_mgr_hosts()
        self.assertEqual(['172.160.6.71'], re)
        cmd = {"prefix": "mgr dump", "format": "json"}
        rados.Rados().mon_command.assert_called_once_with(
            json.dumps(cmd), '')
