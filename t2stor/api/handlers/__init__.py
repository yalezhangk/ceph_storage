#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.alert_group import AlertGroupHandler
from t2stor.api.handlers.alert_group import AlertGroupListHandler
from t2stor.api.handlers.alert_rule import AlertRuleHandler
from t2stor.api.handlers.alert_rule import AlertRuleListHandler
from t2stor.api.handlers.clusters import ClusterDetectHandler
from t2stor.api.handlers.clusters import ClusterHandler
from t2stor.api.handlers.datacenters import DataCenterHandler
from t2stor.api.handlers.datacenters import DataCenterListHandler
from t2stor.api.handlers.datacenters import DataCenterRacksHandler
from t2stor.api.handlers.disks import DiskHandler
from t2stor.api.handlers.disks import DiskListHandler
from t2stor.api.handlers.email_group import EmailGroupHandler
from t2stor.api.handlers.email_group import EmailGroupListHandler
from t2stor.api.handlers.licenses import DownloadlicenseHandler
from t2stor.api.handlers.licenses import LicenseHandler
from t2stor.api.handlers.networks import NetworkListHandler
from t2stor.api.handlers.nodes import NodeHandler
from t2stor.api.handlers.nodes import NodeListHandler
from t2stor.api.handlers.rpc_service import RpcServiceListHandler
from t2stor.api.handlers.sysinfos import SysInfoHandler
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler


def get_routers():
    return [
        (r"/alert_group/", AlertGroupListHandler),
        (r"/alert_group/([0-9]*)/", AlertGroupHandler),
        (r"/alert_rule/", AlertRuleListHandler),
        (r"/alert_rule/([0-9]*)/", AlertRuleHandler),
        (r"/clusters/", ClusterHandler),
        (r"/cluster_detect/", ClusterDetectHandler),
        (r"/datacenters/", DataCenterListHandler),
        (r"/datacenters/([0-9]*)/", DataCenterHandler),
        (r"/datacenters/([0-9]*)/racks/", DataCenterRacksHandler),
        (r"/disks/", DiskListHandler),
        (r"/disks/([0-9]*)/", DiskHandler),
        (r"/email_group/", EmailGroupListHandler),
        (r"/email_group/([0-9]*)/", EmailGroupHandler),
        (r"/licenses/", LicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/networks/", NetworkListHandler),
        (r"/nodes/", NodeListHandler),
        (r"/nodes/([0-9]*)/", NodeHandler),
        (r"/rpc_services/", RpcServiceListHandler),
        (r"/sysinfos/", SysInfoHandler),
        (r"/volumes/", VolumeListHandler),
        (r"/volumes/([0-9]*)/", VolumeHandler),
    ]
