#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.alert_group import AlertGroupHandler
from t2stor.api.handlers.alert_group import AlertGroupListHandler
from t2stor.api.handlers.alert_log import AlertLogHandler
from t2stor.api.handlers.alert_log import AlertLogListHandler
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
from t2stor.api.handlers.log_file import LogFileHandler
from t2stor.api.handlers.log_file import LogFileListHandler
from t2stor.api.handlers.networks import NetworkListHandler
from t2stor.api.handlers.nodes import NodeHandler
from t2stor.api.handlers.nodes import NodeListHandler
from t2stor.api.handlers.pools import PoolHandler
from t2stor.api.handlers.pools import PoolListHandler
from t2stor.api.handlers.racks import RackHandler
from t2stor.api.handlers.racks import RackListHandler
from t2stor.api.handlers.rpc_service import RpcServiceListHandler
from t2stor.api.handlers.service import ServiceListHandler
from t2stor.api.handlers.sysinfos import SysInfoHandler
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler


def get_routers():
    return [
        (r"/alert_groups/", AlertGroupListHandler),
        (r"/alert_groups/([0-9]*)/", AlertGroupHandler),
        (r"/alert_logs/", AlertLogListHandler),
        (r"/alert_logs/([0-9]*)/", AlertLogHandler),
        (r"/alert_rule/", AlertRuleListHandler),
        (r"/alert_rule/([0-9]*)/", AlertRuleHandler),
        (r"/clusters/", ClusterHandler),
        (r"/cluster_detect/", ClusterDetectHandler),
        (r"/datacenters/", DataCenterListHandler),
        (r"/datacenters/([0-9]*)/", DataCenterHandler),
        (r"/datacenters/([0-9]*)/racks/", DataCenterRacksHandler),
        (r"/disks/", DiskListHandler),
        (r"/disks/([0-9]*)/", DiskHandler),
        (r"/email_groups/", EmailGroupListHandler),
        (r"/email_groups/([0-9]*)/", EmailGroupHandler),
        (r"/log_files/", LogFileListHandler),
        (r"/log_files/([0-9]*)/", LogFileHandler),
        (r"/licenses/", LicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/networks/", NetworkListHandler),
        (r"/nodes/", NodeListHandler),
        (r"/nodes/([0-9]*)/", NodeHandler),
        (r"/pools/", PoolListHandler),
        (r"/pools/([0-9]*)/", PoolHandler),
        (r"/racks/", RackListHandler),
        (r"/racks/([0-9]*)/", RackHandler),
        (r"/rpc_services/", RpcServiceListHandler),
        (r"/services/", ServiceListHandler),
        (r"/sysinfos/", SysInfoHandler),
        (r"/volumes/", VolumeListHandler),
        (r"/volumes/([0-9]*)/", VolumeHandler),
    ]
