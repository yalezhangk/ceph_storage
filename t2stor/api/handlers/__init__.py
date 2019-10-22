#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.alert_rule import AlertRuleHandler
from t2stor.api.handlers.alert_rule import AlertRuleListHandler
from t2stor.api.handlers.clusters import ClusterDetectHandler
from t2stor.api.handlers.clusters import ClusterHandler
from t2stor.api.handlers.datacenters import DataCenterHandler
from t2stor.api.handlers.datacenters import DataCenterListHandler
from t2stor.api.handlers.datacenters import DataCenterRacksHandler
from t2stor.api.handlers.disks import DiskListHandler
from t2stor.api.handlers.licenses import DownloadlicenseHandler
from t2stor.api.handlers.licenses import LicenseHandler
from t2stor.api.handlers.networks import NetworkListHandler
from t2stor.api.handlers.nodes import NodeHandler
from t2stor.api.handlers.rpc_service import RpcServiceListHandler
from t2stor.api.handlers.sysinfos import SysInfoHandler
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler


def get_routers():
    return [
        (r"/clusters/", ClusterHandler),
        (r"/cluster_detect/", ClusterDetectHandler),
        (r"/rpc_services/", RpcServiceListHandler),
        (r"/volumes/", VolumeListHandler),
        (r"/volumes/([0-9]*)/", VolumeHandler),
        (r"/licenses/", LicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/alert_rule/", AlertRuleListHandler),
        (r"/alert_rule/([0-9]*)/", AlertRuleHandler),
        (r"/networks/", NetworkListHandler),
        (r"/nodes/([0-9]*)/", NodeHandler),
        (r"/sysinfos/", SysInfoHandler),
        (r"/datacenters/", DataCenterListHandler),
        (r"/datacenters/([0-9]*)/", DataCenterHandler),
        (r"/datacenters/([0-9]*)/racks/", DataCenterRacksHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/disks/", DiskListHandler)
    ]
