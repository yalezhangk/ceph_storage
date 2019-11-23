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
    SHRINK = 'shrink'

    ALL = (CREATING, AVAILABLE, DELETING, ERROR, ERROR_DELETING,
           ERROR_MANAGING, MANAGING, ATTACHING, IN_USE, DETACHING,
           MAINTENANCE, RESTORING_BACKUP, ERROR_RESTORING,
           RESERVED, AWAITING_TRANSFER, BACKING_UP,
           ERROR_BACKING_UP, ERROR_EXTENDING, DOWNLOADING,
           UPLOADING, RETYPING, EXTENDING, DELETED, ACTIVE, SHRINK)


class VolumeStatusField(BaseEnumField):
    AUTO_TYPE = VolumeStatus()


class NodeStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    DEPLOYING_ROLE = 'deploying_role'
    REMOVING_ROLE = 'removing_role'

    # TODO
    INACTIVE = 'inactive'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, DEPLOYING_ROLE,
           REMOVING_ROLE, INACTIVE)


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
    DELETING = 'deleting'
    ERROR = 'error'
    INACTIVE = 'inactive'
    DELETED = 'deleted'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, INACTIVE, DELETED)


class PoolStatusField(BaseEnumField):
    AUTO_TYPE = PoolStatus()


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
    NUMBER = 'number'
    BOOL = 'bool'
    ALL = (STRING, NUMBER, BOOL)


class ConfigTypeField(BaseEnumField):
    AUTO_TYPE = ConfigType()


class DiskStatus(BaseStorEnum):
    AVAILABLE = 'available'
    INUSE = 'inuse'

    ALL = (AVAILABLE, INUSE)


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

    ERROR = 'error'
    FAILED = 'failed'

    ALL = (ACTIVE, INACTIVE, ERROR, FAILED)


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


class OsdStatus(BaseStorEnum):
    CREATING = 'creating'
    DELETING = 'deleting'
    ERROR = 'error'
    AVAILABLE = 'available'

    INUSE = 'inuse'
    ACTIVE = 'active'
    INACTIVE = 'inactive'

    ALL = (CREATING, DELETING, ERROR, AVAILABLE, INUSE, ACTIVE, INACTIVE)


class OsdStatusField(BaseEnumField):
    AUTO_TYPE = OsdStatus()


class OsdDiskTypeField(BaseEnumField):
    AUTO_TYPE = DiskType()


class LogfileType(BaseStorEnum):
    MON = 'mon'
    OSD = 'osd'
    ALL = (MON, OSD)


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
    SMTP_SYSCONFS = 'smtp__sysconfs'
    ALL = (ALERT_GROUP, ALERT_RULE, EMAIL_GROUP, OSD, NODE, POOL, CLUSTER,
           VOLUME, SNAPSHOT, ALERT_LOG, SMTP_SYSCONFS)


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
    POOL_UPDATE_POLICY = 'pool_update_policy'
    CLONE = 'clone'
    SET_ALL_READED = 'set_all_readed'
    ALL = (CREATE, DELETE, MODIFY_ALERT_RULES, MODIFY_EMAIL_GROUPS,
           OPEN_ALERT_RULE, CLOSE_ALERT_RULE, UPDATE, VOLUME_EXTEND,
           VOLUME_SHRINK, VOLUME_ROLLBACK, VOLUME_UNLINK, CLONE)


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
                 AllActionType.CLOSE_ALERT_RULE],

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

            AllResourceType.SMTP_SYSCONFS:
                [AllActionType.UPDATE],

            AllResourceType.POOL:
                [AllActionType.CREATE, AllActionType.UPDATE,
                 AllActionType.DELETE, AllActionType.POOL_ADD_DISK,
                 AllActionType.POOL_DEL_DISK,
                 AllActionType.POOL_UPDATE_POLICY],
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
