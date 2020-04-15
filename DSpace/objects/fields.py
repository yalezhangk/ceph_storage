#!/usr/bin/env python
# -*- coding: utf-8 -*-
from oslo_versionedobjects import fields

BaseEnumField = fields.BaseEnumField
Enum = fields.Enum
Field = fields.Field
FieldType = fields.FieldType


class BaseStorEnum(Enum):
    def __init__(self):
        super(BaseStorEnum, self).__init__(valid_values=self.__class__.ALL)


class ClusterStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    IMPORTING = 'importing'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, IMPORTING)


class ClusterStatusField(BaseEnumField):
    AUTO_TYPE = ClusterStatus()


class VolumeStatus(BaseStorEnum):
    CREATING = 'creating'
    AVAILABLE = 'available'
    DELETING = 'deleting'
    ERROR = 'error'
    ERROR_DELETING = 'error_deleting'
    ERROR_MANAGING = 'error_managing'
    MANAGING = 'managing'
    ATTACHING = 'attaching'
    IN_USE = 'in-use'
    DETACHING = 'detaching'
    MAINTENANCE = 'maintenance'
    RESTORING_BACKUP = 'restoring-backup'
    ERROR_RESTORING = 'error_restoring'
    RESERVED = 'reserved'
    AWAITING_TRANSFER = 'awaiting-transfer'
    BACKING_UP = 'backing-up'
    ERROR_BACKING_UP = 'error_backing-up'
    ERROR_EXTENDING = 'error_extending'
    DOWNLOADING = 'downloading'
    UPLOADING = 'uploading'
    RETYPING = 'retyping'
    EXTENDING = 'extending'
    DELETED = 'deleted'
    ACTIVE = 'active'
    SHRINKING = 'shrinking'
    ROLLBACKING = 'rollbacking'
    UNLINKING = 'unlinking'

    ALL = (CREATING, AVAILABLE, DELETING, ERROR, ERROR_DELETING,
           ERROR_MANAGING, MANAGING, ATTACHING, IN_USE, DETACHING,
           MAINTENANCE, RESTORING_BACKUP, ERROR_RESTORING,
           RESERVED, AWAITING_TRANSFER, BACKING_UP,
           ERROR_BACKING_UP, ERROR_EXTENDING, DOWNLOADING,
           UPLOADING, RETYPING, EXTENDING, DELETED, ACTIVE, SHRINKING,
           ROLLBACKING, UNLINKING)


class VolumeStatusField(BaseEnumField):
    AUTO_TYPE = VolumeStatus()


class BucketStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    DELETED = 'deleted'
    ERROR = 'error'
    PROCESSING = 'processing'

    ALL = (CREATING, ACTIVE, DELETING, DELETED, ERROR,
           PROCESSING)


class BucketStatusField(BaseEnumField):
    AUTO_TYPE = BucketStatus()


class NodeStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    DEPLOYING_ROLE = 'deploying_role'
    REMOVING_ROLE = 'removing_role'
    WARNING = 'warning'
    UPDATING_ROLES = 'updating_roles'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, DEPLOYING_ROLE,
           REMOVING_ROLE, WARNING, UPDATING_ROLES)
    ALIVE = (ACTIVE, DEPLOYING_ROLE, REMOVING_ROLE, WARNING, UPDATING_ROLES)
    IDLE = (ACTIVE, WARNING)


class NodeStatusField(BaseEnumField):
    AUTO_TYPE = NodeStatus()


class VolumeAccessPathStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    DELETED = 'deleted'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, DELETED)


class VolumeAccessPathStatusField(BaseEnumField):
    AUTO_TYPE = VolumeAccessPathStatus()


class PoolStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DEGRADED = 'degraded'
    RECOVERING = 'recovering'
    PROCESSING = 'processing'
    DELETING = 'deleting'
    ERROR = 'error'
    DELETED = 'deleted'
    WARNING = 'warning'

    ALL = (CREATING, ACTIVE, DEGRADED, RECOVERING, PROCESSING, DELETING,
           ERROR, DELETED, WARNING)


class PoolStatusField(BaseEnumField):
    AUTO_TYPE = PoolStatus()


class FaultDomain(BaseStorEnum):
    HOST = 'host'
    RACK = 'rack'
    DATACENTER = 'datacenter'

    ALL = (HOST, RACK, DATACENTER)


class FaultDomainField(BaseEnumField):
    AUTO_TYPE = FaultDomain()


class NetworkStatus(BaseStorEnum):
    UP = 'up'
    DOWN = 'down'

    ALL = (UP, DOWN)


class NetworkStatusField(BaseEnumField):
    AUTO_TYPE = NetworkStatus()


class NetworkType(BaseStorEnum):
    FIBER = 'fiber'
    COPPER = 'copper'

    ALL = (FIBER, COPPER)


class NetworkTypeField(BaseEnumField):
    AUTO_TYPE = NetworkType()


class ConfigType(BaseStorEnum):
    STRING = 'string'
    INT = 'int'
    BOOL = 'bool'
    FLOAT = 'float'
    DICT = 'dict'
    ALL = (STRING, INT, BOOL, DICT)


class ConfigTypeField(BaseEnumField):
    AUTO_TYPE = ConfigType()


class DiskStatus(BaseStorEnum):
    """
    Disk without partitions and don't be mounted, AVAILABLE
    Disk without partitions and is mounted, UNAVAILABLE
    Disk with partitions, UNAVAILABLE
    Disk with partitions and is used by osd or accelerate disk, INUSE
    System Disk, INUSE
    """
    AVAILABLE = 'available'
    UNAVAILABLE = 'unavailable'
    ERROR = 'error'
    INUSE = 'inuse'
    REPLACE_PREPARING = 'replace_preparing'
    REPLACE_PREPARED = 'replace_prepared'
    REPLACING = 'replacing'
    PROCESSING = 'processing'

    REPLACE_STATUS = (REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)
    ALL = (AVAILABLE, UNAVAILABLE, INUSE, ERROR, PROCESSING,
           REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)


class DiskStatusField(BaseEnumField):
    AUTO_TYPE = DiskStatus()


class DiskType(BaseStorEnum):
    SSD = 'ssd'
    HDD = 'hdd'
    NVME = 'nvme'

    ALL = (SSD, HDD, NVME)


class DiskTypeField(BaseEnumField):
    AUTO_TYPE = DiskType()


class DiskRole(BaseStorEnum):
    SYSTEM = 'system'
    DATA = 'data'
    ACCELERATE = 'accelerate'

    ALL = (SYSTEM, DATA, ACCELERATE)


class DiskRoleField(BaseEnumField):
    AUTO_TYPE = DiskRole()


class DiskPartitionStatusField(BaseEnumField):
    AUTO_TYPE = DiskStatus()


class DiskPartitionTypeField(BaseEnumField):
    AUTO_TYPE = DiskType()


class DiskPartitionRole(BaseStorEnum):
    DATA = 'data'
    CACHE = 'cache'
    DB = 'db'
    WAL = 'wal'
    JOURNAL = 'journal'
    MIX = 'mix'

    ALL = (DATA, CACHE, DB, WAL, JOURNAL, MIX)


class DiskPartitionRoleField(BaseEnumField):
    AUTO_TYPE = DiskPartitionRole()


class DiskLedStatus(BaseStorEnum):
    ON = 'on'
    OFF = 'off'

    ALL = (ON, OFF)


class DiskLedStatusField(BaseEnumField):
    AUTO_TYPE = DiskLedStatus()


class ServiceStatus(BaseStorEnum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    STARTING = 'starting'

    ERROR = 'error'
    FAILED = 'failed'

    ALL = (ACTIVE, INACTIVE, STARTING, ERROR, FAILED)


class ServiceStatusField(BaseEnumField):
    AUTO_TYPE = ServiceStatus()


class VolumeSnapshotStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    DELETED = 'deleted'
    ALL = (CREATING, ACTIVE, DELETING, ERROR, DELETED)


class VolumeSnapshotStatusField(BaseEnumField):
    AUTO_TYPE = VolumeSnapshotStatus()


class OsdType(BaseStorEnum):
    FILESTORE = 'filestore'
    BLUESTORE = 'bluestore'

    ALL = (FILESTORE, BLUESTORE)


class OsdTypeField(BaseEnumField):
    AUTO_TYPE = OsdType()


class OsdBackendType(BaseStorEnum):
    T2CE = 't2ce'
    KERNEL = 'kernel'

    ALL = (T2CE, KERNEL)


class OsdStatus(BaseStorEnum):
    ACTIVE = 'active'
    WARNING = 'warning'
    OFFLINE = 'offline'
    CREATING = 'creating'
    DELETING = 'deleting'
    MAINTAIN = 'maintain'
    ERROR = 'error'
    RESTARTING = 'restarting'
    PROCESSING = 'processing'
    REPLACE_PREPARING = 'replace_preparing'
    REPLACE_PREPARED = 'replace_prepared'
    REPLACING = 'replacing'

    OPERATION_STATUS = (CREATING, DELETING, MAINTAIN, RESTARTING, PROCESSING,
                        REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)
    REPLACE_STATUS = (REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)
    OSD_CHECK_IGNORE_STATUS = (CREATING, DELETING, MAINTAIN, PROCESSING,
                               REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)
    ALL = (CREATING, DELETING, ERROR, ACTIVE,
           OFFLINE, RESTARTING, PROCESSING, MAINTAIN, WARNING,
           REPLACE_PREPARING, REPLACE_PREPARED, REPLACING)


class OsdStatusField(BaseEnumField):
    AUTO_TYPE = OsdStatus()


class OsdDiskTypeField(BaseEnumField):
    AUTO_TYPE = DiskType()


class LogfileType(BaseStorEnum):
    MON = 'mon'
    OSD = 'osd'
    RGW = 'rgw'
    MDS = 'mds'
    MGR = 'mgr'
    ALL = (MON, OSD, RGW, MDS, MGR)


class LogfileTypeField(BaseEnumField):
    AUTO_TYPE = LogfileType()


class DictOfNullableField(fields.AutoTypedField):
    AUTO_TYPE = fields.Dict(fields.FieldType(), nullable=True)


class AllResourceType(BaseStorEnum):
    ALERT_GROUP = 'alert_group'
    ALERT_RULE = 'alert_rule'
    EMAIL_GROUP = 'email_group'
    OSD = 'osd'
    NODE = 'node'
    POOL = 'pool'
    CLUSTER = 'cluster'
    VOLUME = 'volume'
    SNAPSHOT = 'snapshot'
    ALERT_LOG = 'alert_log'
    SMTP_SYSCONF = 'smtp_sysconf'
    DISK = 'disk'
    SYSCONFIG = 'sysconfig'
    DATACENTER = 'datacenter'
    RACK = 'rack'
    CEPH_CONFIG = 'ceph_config'
    RADOSGW = 'radosgw'
    RADOSGW_ROUTER = 'radosgw_router'
    SERVICE = 'service'
    NETWORK_INTERFACE = 'network_interface'
    ACCELERATE_DISK = 'accelerate_disk'
    LICENSE = 'license'
    CLIENT_GROUP = 'client_group'
    ACCESS_PATH = 'access_path'
    OBJECT_STORE = 'object_store'
    OBJECT_POLICY = 'object_policy'
    OBJECT_USER = 'object_user'
    OBJECT_BUCKET = 'object_bucket'
    OBJECT_LIFECYCLE = 'object_lifecycle'
    ALL = (ALERT_GROUP, ALERT_RULE, EMAIL_GROUP, OSD, NODE, POOL, CLUSTER,
           VOLUME, SNAPSHOT, ALERT_LOG, SMTP_SYSCONF, DISK, SYSCONFIG,
           DATACENTER, RACK, CEPH_CONFIG, RADOSGW, RADOSGW_ROUTER, SERVICE,
           NETWORK_INTERFACE, ACCELERATE_DISK, LICENSE, CLIENT_GROUP,
           ACCESS_PATH, OBJECT_STORE, OBJECT_POLICY, OBJECT_USER,
           OBJECT_BUCKET, OBJECT_LIFECYCLE)


class AllActionType(BaseStorEnum):
    CREATE = 'create'
    DELETE = 'delete'
    MODIFY_ALERT_RULES = 'modify_alert_rules'
    MODIFY_EMAIL_GROUPS = 'modify_email_groups'
    OPEN_ALERT_RULE = 'open_alert_rule'
    CLOSE_ALERT_RULE = 'close_alert_rule'
    UPDATE = 'update'
    VOLUME_EXTEND = 'volume_extend'
    VOLUME_SHRINK = 'volume_shrink'
    VOLUME_ROLLBACK = 'volume_rollback'
    VOLUME_UNLINK = 'volume_unlink'
    POOL_ADD_DISK = 'pool_add_disk'
    POOL_DEL_DISK = 'pool_del_disk'
    POOL_UNDO = 'pool_undo'
    POOL_UPDATE_POLICY = 'pool_update_policy'
    CLONE = 'clone'
    SET_ALL_READED = 'set_all_readed'
    SET_ROLES = 'set_roles'
    CLUSTER_INCLUDE = 'cluster_include'
    CLUSTER_INCLUDE_CLEAN = 'cluster_include_clean'
    CHANGE_DISK_TYPE = 'change_disk_type'
    DISK_LIGHT = 'disk_light'
    UPDATE_CLOCK_SERVER = 'update_clock_server'
    UPDATE_GATEWAY_CIDR = 'update_gateway_cidr'
    RACK_UPDATE_TOPLOGY = 'rack_update_toplogy'
    NODE_UPDATE_RACK = 'node_update_rack'
    Cluster_PAUSE = 'cluster_pause'
    Cluster_UNPAUSE = 'cluster_unpause'
    OSD_REPLACE = 'osd_replace'  # osd换盘
    DATA_BALANCE_ON = 'data_balance_on'
    DATA_BALANCE_OFF = 'data_balance_off'
    MON_RESTART = 'mon_restart'
    MGR_RESTART = 'mgr_restart'
    MDS_RESTART = 'mds_restart'
    OSD_RESTART = 'osd_restart'
    RGW_RESTART = 'rgw_restart'
    RGW_START = 'rgw_start'
    RGW_STOP = 'rgw_stop'
    RGW_ROUTER_ADD = 'rgw_router_add'
    RGW_ROUTER_REMOVE = 'rgw_router_remove'
    ACC_DISK_CLEAN = 'disk_clean'  # 加速盘清理
    OSD_CLEAN = 'osd_clean'  # osd清理
    ACC_DISK_REBUILD = 'disk_rebuild'  # 重建加速盘
    UPLOAD_LICENSE = 'upload_license'
    DOWNLOAD_LICENSE = 'download_license'
    QUOTA_UPDATE = 'quota_update'  # 更新存储桶配额
    OWNER_UPDATE = 'owner_update'  # 更新存储桶用户
    ACCESS_CONTROL_UPDATE = 'access_control_update'
    UPDATE_VERSIONING_SUSPENDED = 'update_versioning_suspended'
    UPDATE_VERSIONING_OPEN = 'update_versioning_open'
    CLIENT_GROUP_UPDATE_NAME = 'client_group_update_name'
    CLIENT_GROUP_UPDATE_CLIENT = 'client_group_update_client'
    CLIENT_GROUP_UPDATE_CHAP = 'client_group_update_chap'
    ACCESS_PATH_MOUNT_GW = 'access_path_mount_gw'
    ACCESS_PATH_UNMOUNT_GW = 'access_path_unmount_gw'
    ACCESS_PATH_CREATE_MAPPING = 'access_path_create_mapping'
    ACCESS_PATH_REMOVE_MAPPING = 'access_path_remove_mapping'
    ACCESS_PATH_UPDATE_CHAP = 'access_path_update_chap'
    ACCESS_PATH_ADD_VOLUME = 'access_path_add_volume'
    ACCESS_PATH_REMOVE_VOLUME = 'access_path_remove_volume'
    ACCESS_PATH_UPDATE_CLIENT_GROUP = 'access_path_update_client_group'
    OBJECT_STORE_INITIALIZE = 'object_store_initialize'
    SET_DEFAULT = 'set_default'
    SET_COMPRESSION = 'set_compression'
    SET_LIFECYCLE = 'set_lifecycle'
    UPDATE_WORK_TIME = 'update_work_time'
    SET_OBJECT_USER_ENABLE = 'set_object_user_enable'
    SET_OBJECT_USER_DISABLE = 'set_object_user_disable'
    CREATE_KEY = 'create_key'
    DELETE_KEY = 'delete_key'
    UPDATE_KEY = 'update_key'
    UPDATE_EMAIL = 'update_email'
    UPDATE_OP_MASK = 'update_op_mask'
    UPDATE_USER_QUOTA = 'update_user_quota'
    ALL = (CREATE, DELETE, MODIFY_ALERT_RULES, MODIFY_EMAIL_GROUPS,
           OPEN_ALERT_RULE, CLOSE_ALERT_RULE, UPDATE, VOLUME_EXTEND,
           VOLUME_SHRINK, VOLUME_ROLLBACK, VOLUME_UNLINK, CLONE, SET_ROLES,
           CLUSTER_INCLUDE, CHANGE_DISK_TYPE, DISK_LIGHT, UPDATE_CLOCK_SERVER,
           UPDATE_GATEWAY_CIDR, RACK_UPDATE_TOPLOGY, NODE_UPDATE_RACK,
           CLUSTER_INCLUDE_CLEAN, Cluster_PAUSE, Cluster_UNPAUSE, OSD_REPLACE,
           DATA_BALANCE_ON, DATA_BALANCE_OFF, MON_RESTART, MGR_RESTART,
           OSD_RESTART, RGW_START, RGW_STOP, POOL_UNDO, RGW_ROUTER_ADD,
           RGW_ROUTER_REMOVE, ACC_DISK_CLEAN, OSD_CLEAN, ACC_DISK_REBUILD,
           UPLOAD_LICENSE, DOWNLOAD_LICENSE, CLIENT_GROUP_UPDATE_NAME,
           CLIENT_GROUP_UPDATE_CLIENT, CLIENT_GROUP_UPDATE_CHAP,
           ACCESS_PATH_MOUNT_GW, ACCESS_PATH_UNMOUNT_GW,
           ACCESS_PATH_CREATE_MAPPING, ACCESS_PATH_REMOVE_MAPPING,
           ACCESS_PATH_UPDATE_CHAP, ACCESS_PATH_ADD_VOLUME,
           ACCESS_PATH_REMOVE_VOLUME, ACCESS_PATH_UPDATE_CLIENT_GROUP,
           OBJECT_STORE_INITIALIZE, SET_DEFAULT, SET_COMPRESSION,
           SET_LIFECYCLE, UPDATE_WORK_TIME, QUOTA_UPDATE, OWNER_UPDATE,
           CREATE_KEY, ACCESS_CONTROL_UPDATE, UPDATE_VERSIONING_OPEN,
           UPDATE_VERSIONING_SUSPENDED, DELETE_KEY, UPDATE_KEY, UPDATE_EMAIL,
           UPDATE_OP_MASK, UPDATE_USER_QUOTA)


class AllActionStatus(BaseStorEnum):
    SUCCESS = 'success'
    UNDER_WAY = 'under way'  # 进行中
    FAIL = 'fail'
    ALL = (SUCCESS, UNDER_WAY, FAIL)


class CephVersion(BaseStorEnum):
    JEWEL = 'Jewel'
    KRAKEN = 'Kraken'
    LUMINOUS = 'Luminous'
    MIMIC = 'Mimic'
    Nautilus = 'Nautilus'
    T2STOR = 'T2stor'


class ResourceAction(object):

    @classmethod
    def relation_resource_action(cls):
        relation = {
            AllResourceType.ALERT_LOG:
                [AllActionType.SET_ALL_READED, AllActionType.DELETE],

            AllResourceType.ALERT_GROUP:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.MODIFY_ALERT_RULES,
                 AllActionType.MODIFY_EMAIL_GROUPS],

            AllResourceType.ALERT_RULE:
                [AllActionType.OPEN_ALERT_RULE,
                 AllActionType.CLOSE_ALERT_RULE,
                 AllActionType.UPDATE],

            AllResourceType.EMAIL_GROUP:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE],

            AllResourceType.SNAPSHOT:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.CLONE],

            AllResourceType.VOLUME:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.VOLUME_EXTEND,
                 AllActionType.VOLUME_SHRINK, AllActionType.VOLUME_ROLLBACK,
                 AllActionType.VOLUME_UNLINK],

            AllResourceType.SMTP_SYSCONF:
                [AllActionType.UPDATE],

            AllResourceType.POOL:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.POOL_ADD_DISK,
                 AllActionType.POOL_DEL_DISK,
                 AllActionType.POOL_UPDATE_POLICY,
                 AllActionType.POOL_UNDO],

            AllResourceType.NODE:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.SET_ROLES, AllActionType.NODE_UPDATE_RACK,
                 AllActionType.MON_RESTART, AllActionType.MGR_RESTART,
                 AllActionType.MDS_RESTART],

            AllResourceType.OSD:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.OSD_RESTART, AllActionType.OSD_CLEAN,
                 AllActionType.OSD_REPLACE],

            AllResourceType.DISK:
                [AllActionType.CHANGE_DISK_TYPE, AllActionType.DISK_LIGHT],

            AllResourceType.SYSCONFIG:
                [AllActionType.UPDATE_CLOCK_SERVER,
                 AllActionType.UPDATE_GATEWAY_CIDR],

            AllResourceType.DATACENTER:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE],

            AllResourceType.RACK:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.RACK_UPDATE_TOPLOGY],

            AllResourceType.CLUSTER:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.CLUSTER_INCLUDE,
                 AllActionType.CLUSTER_INCLUDE_CLEAN,
                 AllActionType.DATA_BALANCE_ON,
                 AllActionType.DATA_BALANCE_OFF,
                 AllActionType.Cluster_PAUSE,
                 AllActionType.Cluster_UNPAUSE],

            AllResourceType.CEPH_CONFIG:
                [AllActionType.UPDATE],

            AllResourceType.RADOSGW:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.RGW_START, AllActionType.RGW_STOP],

            AllResourceType.RADOSGW_ROUTER:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.RGW_ROUTER_ADD,
                 AllActionType.RGW_ROUTER_REMOVE],

            AllResourceType.ACCELERATE_DISK:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.ACC_DISK_CLEAN, AllActionType.ACC_DISK_REBUILD],
            AllResourceType.LICENSE:
                [AllActionType.UPLOAD_LICENSE, AllActionType.DOWNLOAD_LICENSE],

            AllResourceType.CLIENT_GROUP:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.UPDATE, AllActionType.CLIENT_GROUP_UPDATE_NAME,
                 AllActionType.CLIENT_GROUP_UPDATE_CLIENT,
                 AllActionType.CLIENT_GROUP_UPDATE_CHAP],

            AllResourceType.ACCESS_PATH:
                [AllActionType.CREATE, AllActionType.DELETE,
                 AllActionType.UPDATE,
                 AllActionType.ACCESS_PATH_MOUNT_GW,
                 AllActionType.ACCESS_PATH_UNMOUNT_GW,
                 AllActionType.ACCESS_PATH_CREATE_MAPPING,
                 AllActionType.ACCESS_PATH_REMOVE_MAPPING,
                 AllActionType.ACCESS_PATH_UPDATE_CHAP,
                 AllActionType.ACCESS_PATH_ADD_VOLUME,
                 AllActionType.ACCESS_PATH_REMOVE_VOLUME,
                 AllActionType.ACCESS_PATH_UPDATE_CLIENT_GROUP],

            AllResourceType.OBJECT_POLICY:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.SET_DEFAULT,
                 AllActionType.SET_COMPRESSION],

            AllResourceType.OBJECT_STORE:
                [AllActionType.OBJECT_STORE_INITIALIZE],

            AllResourceType.OBJECT_BUCKET:
                [AllActionType.SET_LIFECYCLE, AllActionType.CREATE,
                 AllActionType.UPDATE_VERSIONING_SUSPENDED,
                 AllActionType.UPDATE_VERSIONING_OPEN,
                 AllActionType.ACCESS_CONTROL_UPDATE,
                 AllActionType.OWNER_UPDATE,
                 AllActionType.QUOTA_UPDATE,
                 AllActionType.DELETE],

            AllResourceType.OBJECT_LIFECYCLE:
                [AllActionType.UPDATE_WORK_TIME],

            AllResourceType.OBJECT_USER:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.UPDATE_EMAIL,
                 AllResourceType.SET_OBJECT_USER_ENABLE,
                 AllResourceType.SET_OBJECT_USER_DISABLE,
                 AllResourceType.CREATE_KEY,
                 AllResourceType.DELETE_KEY,
                 AllResourceType.UPDATE_KEY,
                 AllResourceType.UPDATE_OP_MASK,
                 AllResourceType.UPDATE_USER_QUOTA]
        }
        return relation


class AlertLogLevel(BaseStorEnum):
    INFO = 'INFO'
    WARN = 'WARN'
    ERROR = 'ERROR'
    FATAL = 'FATAL'
    ALL = (INFO, WARN, ERROR, FATAL)


class TaskStatus(BaseStorEnum):
    SUCCESS = 'success'
    RUNNING = 'running'
    FAILED = 'failed'
    ROLLBACKING = 'rollbacking'
    ALL = (SUCCESS, RUNNING, FAILED, ROLLBACKING)


class TaskStatusField(BaseEnumField):
    AUTO_TYPE = TaskStatus()


class RadosgwStatus(BaseStorEnum):
    CREATING = 'creating'
    DELETING = 'deleting'
    STOPPING = "stopping"
    STOPPED = "stopped"
    STARTING = "starting"
    ERROR = 'error'

    ACTIVE = 'active'
    INACTIVE = 'inactive'

    ALL = (CREATING, DELETING, STOPPING, STOPPED, STARTING, STARTING, ERROR,
           ACTIVE, INACTIVE)


class RadosgwStatusField(BaseEnumField):
    AUTO_TYPE = RadosgwStatus()


class RadosgwRouterStatus(BaseStorEnum):
    CREATING = 'creating'
    DELETING = 'deleting'
    UPDATING = 'updating'
    ERROR = 'error'

    ACTIVE = 'active'
    INACTIVE = 'inactive'

    ALL = (CREATING, DELETING, UPDATING, ERROR, ACTIVE, INACTIVE)


class RadosgwRouterStatusField(BaseEnumField):
    AUTO_TYPE = RadosgwRouterStatus()


class RouterServiceStatus(BaseStorEnum):
    CREATING = 'creating'
    DELETING = 'deleting'
    STARTING = 'starting'
    ERROR = 'error'

    ACTIVE = 'active'
    INACTIVE = 'inactive'

    ALL = (CREATING, DELETING, STARTING, ERROR, ACTIVE, INACTIVE)


class RouterServiceStatusField(BaseEnumField):
    AUTO_TYPE = RouterServiceStatus()


class ConfigKey(BaseStorEnum):
    ENABLE_CEPH_REPO = 'enable_ceph_repo'
    REMOVE_ANOTHER_REPO = 'remove_another_repo'
    CEPH_VERSION = 'ceph_version'
    IMAGE_NAME = 'image_name'
    IMAGE_NAMESPACE = 'image_namespace'
    DSPACE_VERSION = 'dspace_version'
    ADMIN_IP_ADDRESS = 'admin_ip_address'
    AGENT_PORT = 'agent_port'
    ADMIN_PORT = 'admin_port'
    DSPACE_REPO = 'dspace_repo'
    CONFIG_DIR = 'config_dir'
    CONFIG_DIR_CONTAINER = 'config_dir_container'
    LOG_DIR = 'log_dir'
    LOG_DIR_CONTAINER = 'log_dir_container'
    ADMIN_IPS = 'admin_ips'
    MAX_OSD_NUM = 'max_osd_num'
    MAX_MONITOR_NUM = 'max_monitor_num'
    DSPACE_DIR = 'dspace_dir'
    NODE_EXPORTER_PORT = 'node_exporter_port'
    DEBUG_MODE = 'debug_mode'
    CEPH_MONITOR_PORT = 'ceph_monitor_port'
    MGR_DSPACE_PORT = 'mgr_dspace_port'
    DSA_SOCKET_FILE = 'dsa_socket_file'
    DSA_LIB_DIR = 'dsa_lib_dir'
    UDEV_DIR = 'udev_dir'
    ENABLE_CEPHX = 'enable_cephx'
    OS_DISTRO = 'os_distro'
    CEPH_VERSION_NAME = "ceph_version_name"
    DISABLE_LICENSE = 'disable_license'
    OBJECT_STORE_INIT = 'object_store_init'
    OBJECT_META_POOL = 'object_meta_pool'
    ENABLE_OBJS_PAGE = 'enable_objects_page'
    POOL_ID_SAME_AS_NAME = 'pool_id_same_as_name'
    ENABLE_BLOCKS_PAGE = 'enable_blocks_page'


class DSMStatus(BaseStorEnum):
    INIT = "init"
    ACTIVE = "active"
    BACKUP = "backup"


class ObjectStoreStatus(BaseStorEnum):
    INITIALIZING = "initializing"
    ACTIVE = 'active'
    ERROR = 'error'

    ALL = (INITIALIZING, ACTIVE, ERROR)


class ObjectStoreStatusField(BaseEnumField):
    AUTO_TYPE = ObjectStoreStatus()


class PoolRole(BaseStorEnum):
    INDEX = "index"
    DATA = "data"
    OBJECT_META = "object_meta"

    ALL = (INDEX, DATA, OBJECT_META)


class PoolRoleField(BaseEnumField):
    AUTO_TYPE = PoolRole()


class CompressionAlgorithm(BaseStorEnum):
    # 压缩算法
    NULL = None
    SNAPPY = 'snappy'
    ZLIB = 'zlib'
    ZSTD = 'zstd'
    ALL = (NULL, SNAPPY, ZLIB, ZSTD)


class ObjectUserStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    SUSPENDED = 'suspended'

    ALL = (CREATING, ACTIVE, DELETING, SUSPENDED,
           ERROR)


class ObjectUserStatusField(BaseEnumField):
    AUTO_TYPE = ObjectUserStatus()
