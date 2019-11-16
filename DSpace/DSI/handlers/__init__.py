#!/usr/bin/env python
# -*- coding: utf-8 -*-
from DSpace.DSI.handlers.action_log import ActionLogHandler
from DSpace.DSI.handlers.action_log import ActionLogListHandler
from DSpace.DSI.handlers.action_log import ResourceActionHandler
from DSpace.DSI.handlers.alert_group import AlertGroupHandler
from DSpace.DSI.handlers.alert_group import AlertGroupListHandler
from DSpace.DSI.handlers.alert_log import AlertLogActionHandler
from DSpace.DSI.handlers.alert_log import AlertLogHandler
from DSpace.DSI.handlers.alert_log import AlertLogListHandler
from DSpace.DSI.handlers.alert_log import AlertTpyeCountHandler
from DSpace.DSI.handlers.alert_log import ReceiveAlertMessageHandler
from DSpace.DSI.handlers.alert_rule import AlertRuleHandler
from DSpace.DSI.handlers.alert_rule import AlertRuleListHandler
from DSpace.DSI.handlers.ceph_config import CephConfigActionHandler
from DSpace.DSI.handlers.ceph_config import CephConfigContentHandler
from DSpace.DSI.handlers.ceph_config import CephConfigListHandler
from DSpace.DSI.handlers.clusters import ClusterAdminNodesHandler
from DSpace.DSI.handlers.clusters import ClusterCheckAdminNodeHandler
from DSpace.DSI.handlers.clusters import ClusterDetectHandler
from DSpace.DSI.handlers.clusters import ClusterHandler
from DSpace.DSI.handlers.clusters import ClusterHistoryMetricsHandler
from DSpace.DSI.handlers.clusters import ClusterHostStatus
from DSpace.DSI.handlers.clusters import ClusterMetricsHandler
from DSpace.DSI.handlers.clusters import ClusterOsdStatus
from DSpace.DSI.handlers.clusters import ClusterPoolStatus
from DSpace.DSI.handlers.clusters import ClusterServiceStatus
from DSpace.DSI.handlers.datacenters import DataCenterHandler
from DSpace.DSI.handlers.datacenters import DataCenterListHandler
from DSpace.DSI.handlers.datacenters import DataCenterTreeHandler
from DSpace.DSI.handlers.disk_partitions import DiskPartitionListHandler
from DSpace.DSI.handlers.disks import DiskActionHandler
from DSpace.DSI.handlers.disks import DiskAvailableListHandler
from DSpace.DSI.handlers.disks import DiskHandler
from DSpace.DSI.handlers.disks import DiskListHandler
from DSpace.DSI.handlers.disks import DiskPerfHandler
from DSpace.DSI.handlers.disks import DiskPerfHistoryHandler
from DSpace.DSI.handlers.disks import DiskSmartHandler
from DSpace.DSI.handlers.email_group import EmailGroupHandler
from DSpace.DSI.handlers.email_group import EmailGroupListHandler
from DSpace.DSI.handlers.licenses import DownloadlicenseHandler
from DSpace.DSI.handlers.licenses import LicenseHandler
from DSpace.DSI.handlers.log_file import LogFileHandler
from DSpace.DSI.handlers.log_file import LogFileListHandler
from DSpace.DSI.handlers.networks import NetworkListHandler
from DSpace.DSI.handlers.nodes import NodeCheckHandler
from DSpace.DSI.handlers.nodes import NodeHandler
from DSpace.DSI.handlers.nodes import NodeInfoHandler
from DSpace.DSI.handlers.nodes import NodeListBareNodeHandler
from DSpace.DSI.handlers.nodes import NodeListHandler
from DSpace.DSI.handlers.nodes import NodeMetricsHistroyMonitorHandler
from DSpace.DSI.handlers.nodes import NodeMetricsHistroyNetworkHandler
from DSpace.DSI.handlers.nodes import NodeMetricsMonitorHandler
from DSpace.DSI.handlers.nodes import NodeMetricsNetworkHandler
from DSpace.DSI.handlers.nodes import NodeRoleHandler
from DSpace.DSI.handlers.osds import OsdDiskMetricsHandler
from DSpace.DSI.handlers.osds import OsdHandler
from DSpace.DSI.handlers.osds import OsdHistoryDiskMetricsHandler
from DSpace.DSI.handlers.osds import OsdListHandler
from DSpace.DSI.handlers.osds import OsdMetricsHandler
from DSpace.DSI.handlers.osds import OsdMetricsHistoryHandler
from DSpace.DSI.handlers.pools import PoolCapacityHandler
from DSpace.DSI.handlers.pools import PoolDecreaseDiskHandler
from DSpace.DSI.handlers.pools import PoolHandler
from DSpace.DSI.handlers.pools import PoolIncreaseDiskHandler
from DSpace.DSI.handlers.pools import PoolListHandler
from DSpace.DSI.handlers.pools import PoolMetricsHandler
from DSpace.DSI.handlers.pools import PoolMetricsHistoryHandler
from DSpace.DSI.handlers.pools import PoolOsdsHandler
from DSpace.DSI.handlers.pools import PoolPolicyHandler
from DSpace.DSI.handlers.probe import ProbeClusterNodesHandler
from DSpace.DSI.handlers.racks import RackHandler
from DSpace.DSI.handlers.racks import RackListHandler
from DSpace.DSI.handlers.rpc_service import RpcServiceListHandler
from DSpace.DSI.handlers.service import ServiceListHandler
from DSpace.DSI.handlers.sysinfos import SmtpHandler
from DSpace.DSI.handlers.sysinfos import SmtpTestHandler
from DSpace.DSI.handlers.sysinfos import SysInfoHandler
from DSpace.DSI.handlers.user import PermissionHandler
from DSpace.DSI.handlers.user import UserHandler
from DSpace.DSI.handlers.user import UserListHandler
from DSpace.DSI.handlers.user import UserLoginHandler
from DSpace.DSI.handlers.user import UserLogoutHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathAddVolumeHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathChangeClientGroupHandler
from DSpace.DSI.handlers.volume_access_paths import VolumeAccessPathChapHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathCreateMappingHandler
from DSpace.DSI.handlers.volume_access_paths import VolumeAccessPathHandler
from DSpace.DSI.handlers.volume_access_paths import VolumeAccessPathListHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathMountGWHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathRemoveMappingHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathRemoveVolumeHandler
from DSpace.DSI.handlers.volume_access_paths import \
    VolumeAccessPathUnmountGWHandler
from DSpace.DSI.handlers.volume_client_groups import VolumeClientByGroup
from DSpace.DSI.handlers.volume_client_groups import VolumeClientGroupHandler
from DSpace.DSI.handlers.volume_client_groups import \
    VolumeClientGroupListHandler
from DSpace.DSI.handlers.volume_client_groups import \
    VolumeClientGroupSetMutualChapHandler
from DSpace.DSI.handlers.volume_snapshot import VolumeSnapshotActionHandler
from DSpace.DSI.handlers.volume_snapshot import VolumeSnapshotHandler
from DSpace.DSI.handlers.volume_snapshot import VolumeSnapshotListHandler
from DSpace.DSI.handlers.volumes import VolumeActionHandler
from DSpace.DSI.handlers.volumes import VolumeHandler
from DSpace.DSI.handlers.volumes import VolumeListHandler


def get_routers():
    return [
        (r"/action_logs/", ActionLogListHandler),
        (r"/action_logs/([0-9]*)/", ActionLogHandler),
        (r"/action_logs/resource_action/", ResourceActionHandler),
        (r"/alert_groups/", AlertGroupListHandler),
        (r"/alert_groups/([0-9]*)/", AlertGroupHandler),
        (r"/alert_logs/", AlertLogListHandler),
        (r"/alert_logs/([0-9]*)/", AlertLogHandler),
        (r"/alert_logs/action/", AlertLogActionHandler),
        (r"/alert_logs/messages/", ReceiveAlertMessageHandler),
        (r"/alert_logs/type_count/", AlertTpyeCountHandler),
        (r"/alert_rules/", AlertRuleListHandler),
        (r"/alert_rules/([0-9]*)/", AlertRuleHandler),
        (r"/ceph_configs/", CephConfigListHandler),
        (r"/ceph_configs/action/", CephConfigActionHandler),
        (r"/ceph_configs/content/", CephConfigContentHandler),
        (r"/clusters/", ClusterHandler),
        (r"/clusters/get_admin_nodes/", ClusterAdminNodesHandler),
        (r"/clusters/check_admin_node_status/", ClusterCheckAdminNodeHandler),
        (r"/cluster_detect/", ClusterDetectHandler),
        (r"/clusters/history_metrics/", ClusterHistoryMetricsHandler),
        (r"/clusters/host_status/", ClusterHostStatus),
        (r"/clusters/metrics/", ClusterMetricsHandler),
        (r"/clusters/osd_status/", ClusterOsdStatus),
        (r"/clusters/pool_status/", ClusterPoolStatus),
        (r"/clusters/services_status/", ClusterServiceStatus),
        (r"/datacenters/", DataCenterListHandler),
        (r"/datacenters/tree/", DataCenterTreeHandler),
        (r"/datacenters/([0-9]*)/", DataCenterHandler),
        (r"/disks/", DiskListHandler),
        (r"/disks/([0-9]*)/", DiskHandler),
        (r"/disks/([0-9]*)/action/", DiskActionHandler),
        (r"/disks/([0-9]*)/perf/", DiskPerfHandler),
        (r"/disks/([0-9]*)/history_perf/", DiskPerfHistoryHandler),
        (r"/disks/([0-9]*)/smart/", DiskSmartHandler),
        (r"/disk_available/", DiskAvailableListHandler),
        (r"/disk_partitions/", DiskPartitionListHandler),
        (r"/email_groups/", EmailGroupListHandler),
        (r"/email_groups/([0-9]*)/", EmailGroupHandler),
        (r"/login/", UserLoginHandler),
        (r"/logout/", UserLogoutHandler),
        (r"/log_files/", LogFileListHandler),
        (r"/log_files/([0-9]*)/", LogFileHandler),
        (r"/licenses/", LicenseHandler),
        (r"/licenses/download_file/", DownloadlicenseHandler),
        (r"/networks/", NetworkListHandler),
        (r"/nodes/", NodeListHandler),
        (r"/nodes/bare_node/", NodeListBareNodeHandler),
        (r"/nodes/get_infos/", NodeInfoHandler),
        (r"/nodes/check_node/", NodeCheckHandler),
        (r"/nodes/([0-9]*)/", NodeHandler),
        (r"/nodes/([0-9]*)/role/", NodeRoleHandler),
        (r"/nodes/([0-9]*)/metrics/", NodeMetricsMonitorHandler),
        (r"/nodes/([0-9]*)/history_metrics/",
            NodeMetricsHistroyMonitorHandler),
        (r"/nodes/([0-9]*)/history_network/",
            NodeMetricsHistroyNetworkHandler),
        (r"/nodes/([0-9]*)/metrics/network/", NodeMetricsNetworkHandler),
        (r"/osds/", OsdListHandler),
        (r"/osds/([0-9]*)/", OsdHandler),
        (r"/osds/([0-9]*)/disk_metrics/",
            OsdDiskMetricsHandler),
        (r"/osds/([0-9]*)/history_disk_metrics/",
            OsdHistoryDiskMetricsHandler),
        (r"/osds/([0-9]*)/history_metrics/", OsdMetricsHistoryHandler),
        (r"/osds/([0-9]*)/metrics/", OsdMetricsHandler),
        (r"/pools/", PoolListHandler),
        (r"/pools/([0-9]*)/", PoolHandler),
        (r"/pools/([0-9]*)/capacity/", PoolCapacityHandler),
        (r"/pools/([0-9]*)/decrease_disk/", PoolDecreaseDiskHandler),
        (r"/pools/([0-9]*)/increase_disk/", PoolIncreaseDiskHandler),
        (r"/pools/([0-9]*)/history_metrics/", PoolMetricsHistoryHandler),
        (r"/pools/([0-9]*)/metrics/", PoolMetricsHandler),
        (r"/pools/([0-9]*)/osds/", PoolOsdsHandler),
        (r"/pools/([0-9]*)/update_security_policy/", PoolPolicyHandler),
        (r"/probe_cluster_nodes/", ProbeClusterNodesHandler),
        (r"/racks/", RackListHandler),
        (r"/racks/([0-9]*)/", RackHandler),
        (r"/rpc_services/", RpcServiceListHandler),
        (r"/services/", ServiceListHandler),
        (r"/sysconfs/smtp/", SmtpHandler),
        (r"/sysconfs/mail/test/", SmtpTestHandler),
        (r"/sysinfos/", SysInfoHandler),
        (r"/users/", UserListHandler),
        (r"/users/([0-9]*)/", UserHandler),
        (r"/permissions/", PermissionHandler),
        (r"/volume_access_paths/", VolumeAccessPathListHandler),
        (r"/volume_access_paths/([0-9]*)/", VolumeAccessPathHandler),
        (r"/volume_access_paths/([0-9]*)/add_volume/",
            VolumeAccessPathAddVolumeHandler),
        (r"/volume_access_paths/([0-9]*)/change_client_group/",
            VolumeAccessPathChangeClientGroupHandler),
        (r"/volume_access_paths/([0-9]*)/create_mapping/",
            VolumeAccessPathCreateMappingHandler),
        (r"/volume_access_paths/([0-9]*)/mount_gw/",
            VolumeAccessPathMountGWHandler),
        (r"/volume_access_paths/([0-9]*)/remove_mapping/",
            VolumeAccessPathRemoveMappingHandler),
        (r"/volume_access_paths/([0-9]*)/remove_volume/",
            VolumeAccessPathRemoveVolumeHandler),
        (r"/volume_access_paths/([0-9]*)/set_chap/",
            VolumeAccessPathChapHandler),
        (r"/volume_access_paths/([0-9]*)/unmount_gw/",
            VolumeAccessPathUnmountGWHandler),
        (r"/volume_client_groups/", VolumeClientGroupListHandler),
        (r"/volume_client_groups/([0-9]*)/", VolumeClientGroupHandler),
        (r"/volume_client_groups/([0-9]*)/clients/", VolumeClientByGroup),
        (r"/volume_client_groups/([0-9]*)/set_mutual_chap/",
            VolumeClientGroupSetMutualChapHandler),
        (r"/volume_snapshots/", VolumeSnapshotListHandler),
        (r"/volume_snapshots/([0-9]*)/", VolumeSnapshotHandler),
        (r"/volume_snapshots/([0-9]*)/action/", VolumeSnapshotActionHandler),
        (r"/volumes/", VolumeListHandler),
        (r"/volumes/([0-9]*)/", VolumeHandler),
        (r"/volumes/([0-9]*)/action/", VolumeActionHandler),
    ]
