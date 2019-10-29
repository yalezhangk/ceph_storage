#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.alert_group import AlertGroupHandler
from t2stor.api.handlers.alert_group import AlertGroupListHandler
from t2stor.api.handlers.alert_log import AlertLogHandler
from t2stor.api.handlers.alert_log import AlertLogListHandler
from t2stor.api.handlers.alert_rule import AlertRuleHandler
from t2stor.api.handlers.alert_rule import AlertRuleListHandler
from t2stor.api.handlers.ceph_config import CephConfigActionHandler
from t2stor.api.handlers.ceph_config import CephConfigContentHandler
from t2stor.api.handlers.ceph_config import CephConfigListHandler
from t2stor.api.handlers.clusters import ClusterDetectHandler
from t2stor.api.handlers.clusters import ClusterHandler
from t2stor.api.handlers.datacenters import DataCenterHandler
from t2stor.api.handlers.datacenters import DataCenterListHandler
from t2stor.api.handlers.datacenters import DataCenterRacksHandler
from t2stor.api.handlers.disk_partitions import DiskPartitionListHandler
from t2stor.api.handlers.disks import DiskActionHandler
from t2stor.api.handlers.disks import DiskHandler
from t2stor.api.handlers.disks import DiskListHandler
from t2stor.api.handlers.disks import DiskSmartHandler
from t2stor.api.handlers.email_group import EmailGroupHandler
from t2stor.api.handlers.email_group import EmailGroupListHandler
from t2stor.api.handlers.licenses import DownloadlicenseHandler
from t2stor.api.handlers.licenses import LicenseHandler
from t2stor.api.handlers.log_file import LogFileHandler
from t2stor.api.handlers.log_file import LogFileListHandler
from t2stor.api.handlers.networks import NetworkListHandler
from t2stor.api.handlers.nodes import NodeHandler
from t2stor.api.handlers.nodes import NodeListHandler
from t2stor.api.handlers.osds import OsdHandler
from t2stor.api.handlers.osds import OsdListHandler
from t2stor.api.handlers.pools import PoolCapacityHandler
from t2stor.api.handlers.pools import PoolDecreaseDiskHandler
from t2stor.api.handlers.pools import PoolHandler
from t2stor.api.handlers.pools import PoolHistoryMetricsHandler
from t2stor.api.handlers.pools import PoolIncreaseDiskHandler
from t2stor.api.handlers.pools import PoolListHandler
from t2stor.api.handlers.pools import PoolMetricsHandler
from t2stor.api.handlers.pools import PoolOsdsHandler
from t2stor.api.handlers.pools import PoolPolicyHandler
from t2stor.api.handlers.racks import RackHandler
from t2stor.api.handlers.racks import RackListHandler
from t2stor.api.handlers.rpc_service import RpcServiceListHandler
from t2stor.api.handlers.service import ServiceListHandler
from t2stor.api.handlers.sysinfos import SmtpHandler
from t2stor.api.handlers.sysinfos import SysInfoHandler
from t2stor.api.handlers.volume_access_paths import VolumeAccessPathHandler
from t2stor.api.handlers.volume_access_paths import VolumeAccessPathListHandler
from t2stor.api.handlers.volume_client_groups import VolumeClientByGroup
from t2stor.api.handlers.volume_client_groups import VolumeClientGroupHandler
from t2stor.api.handlers.volume_client_groups import \
    VolumeClientGroupListHandler
from t2stor.api.handlers.volume_snapshot import VolumeSnapshotActionHandler
from t2stor.api.handlers.volume_snapshot import VolumeSnapshotHandler
from t2stor.api.handlers.volume_snapshot import VolumeSnapshotListHandler
from t2stor.api.handlers.volumes import VolumeActionHandler
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler


def get_routers():
    return [
        (r"/alert_groups/", AlertGroupListHandler),
        (r"/alert_groups/([0-9]*)/", AlertGroupHandler),
        (r"/alert_logs/", AlertLogListHandler),
        (r"/alert_logs/([0-9]*)/", AlertLogHandler),
        (r"/alert_rules/", AlertRuleListHandler),
        (r"/alert_rules/([0-9]*)/", AlertRuleHandler),
        (r"/ceph_configs/", CephConfigListHandler),
        (r"/ceph_configs/action/", CephConfigActionHandler),
        (r"/ceph_configs/content/", CephConfigContentHandler),
        (r"/clusters/", ClusterHandler),
        (r"/cluster_detect/", ClusterDetectHandler),
        (r"/datacenters/", DataCenterListHandler),
        (r"/datacenters/([0-9]*)/", DataCenterHandler),
        (r"/datacenters/([0-9]*)/racks/", DataCenterRacksHandler),
        (r"/disks/", DiskListHandler),
        (r"/disks/([0-9]*)/", DiskHandler),
        (r"/disks/([0-9]*)/action/", DiskActionHandler),
        (r"/disks/([0-9]*)/smart/", DiskSmartHandler),
        (r"/disk_partitions/", DiskPartitionListHandler),
        (r"/email_groups/", EmailGroupListHandler),
        (r"/email_groups/([0-9]*)/", EmailGroupHandler),
        (r"/log_files/", LogFileListHandler),
        (r"/log_files/([0-9]*)/", LogFileHandler),
        (r"/licenses/", LicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/networks/", NetworkListHandler),
        (r"/nodes/", NodeListHandler),
        (r"/nodes/([0-9]*)/", NodeHandler),
        (r"/osds/", OsdListHandler),
        (r"/osds/([0-9]*)/", OsdHandler),
        (r"/pools/", PoolListHandler),
        (r"/pools/([0-9]*)/", PoolHandler),
        (r"/pools/([0-9]*)/capacity/", PoolCapacityHandler),
        (r"/pools/([0-9]*)/decrease_disk/", PoolDecreaseDiskHandler),
        (r"/pools/([0-9]*)/increase_disk/", PoolIncreaseDiskHandler),
        (r"/pools/([0-9]*)/histroy_metrics/", PoolHistoryMetricsHandler),
        (r"/pools/([0-9]*)/metrics/", PoolMetricsHandler),
        (r"/pools/([0-9]*)/osds/", PoolOsdsHandler),
        (r"/pools/([0-9]*)/update_security_policy/", PoolPolicyHandler),
        (r"/racks/", RackListHandler),
        (r"/racks/([0-9]*)/", RackHandler),
        (r"/rpc_services/", RpcServiceListHandler),
        (r"/services/", ServiceListHandler),
        (r"/sysconfs/set_smtp/", SmtpHandler),
        (r"/sysconfs/smtp/", SmtpHandler),
        (r"/sysinfos/", SysInfoHandler),
        (r"/volume_access_paths/", VolumeAccessPathListHandler),
        (r"/volume_access_paths/([0-9]*)/", VolumeAccessPathHandler),
        (r"/volume_client_groups/", VolumeClientGroupListHandler),
        (r"/volume_client_groups/([0-9]*)/", VolumeClientGroupHandler),
        (r"/volume_client_groups/([0-9]*)/clients", VolumeClientByGroup),
        (r"/volume_snapshots/", VolumeSnapshotListHandler),
        (r"/volume_snapshots/([0-9]*)/", VolumeSnapshotHandler),
        (r"/volume_snapshots/([0-9]*)/action/", VolumeSnapshotActionHandler),
        (r"/volumes/", VolumeListHandler),
        (r"/volumes/([0-9]*)/", VolumeHandler),
        (r"/volumes/([0-9]*)/action/", VolumeActionHandler),
    ]
