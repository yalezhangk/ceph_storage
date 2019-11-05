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

    ALL = (CREATING, AVAILABLE, DELETING, ERROR, ERROR_DELETING,
           ERROR_MANAGING, MANAGING, ATTACHING, IN_USE, DETACHING,
           MAINTENANCE, RESTORING_BACKUP, ERROR_RESTORING,
           RESERVED, AWAITING_TRANSFER, BACKING_UP,
           ERROR_BACKING_UP, ERROR_EXTENDING, DOWNLOADING,
           UPLOADING, RETYPING, EXTENDING, DELETED, ACTIVE)


class VolumeStatusField(BaseEnumField):
    AUTO_TYPE = VolumeStatus()


class NodeStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'
    DEPLOYING = 'deploying'

    ALL = (CREATING, ACTIVE, DELETING, ERROR, DEPLOYING)


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


class SysConfigType(BaseStorEnum):
    STRING = 'string'
    NUMBER = 'number'
    BOOL = 'bool'
    ALL = (STRING, NUMBER, BOOL)


class SysConfigTypeField(BaseEnumField):
    AUTO_TYPE = SysConfigType()


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
    CACHE = 'cache'
    DB = 'db'
    WAL = 'wal'
    JOURNAL = 'journal'
    MIX = 'mix'

    ALL = (CACHE, DB, WAL, JOURNAL, MIX)


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

    ALL = (ACTIVE, INACTIVE)


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
    UP = 'up'
    DOWN = 'down'
    CREATING = 'creating'
    DELETING = 'deleting'
    ERROR = 'error'

    ALL = (UP, DOWN, CREATING, DELETING, ERROR)


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
    EMAIL_GROUP = 'email_group'
    ALL = (EMAIL_GROUP, )


class AllActionType(BaseStorEnum):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    ALL = (CREATE, UPDATE, DELETE)


class AllActionStatus(BaseStorEnum):
    SUCCESS = 'success'
    UNDER_WAY = 'under way'  # 进行中
    FAIL = 'fail'
    ALL = (SUCCESS, UNDER_WAY, FAIL)


class ResourceAction(object):

    @classmethod
    def relation_resource_action(cls):
        relation = {
            AllResourceType.EMAIL_GROUP: {
                AllActionType.CREATE: AllActionType.CREATE,
                AllActionType.UPDATE: AllActionType.UPDATE,
                AllActionType.DELETE: AllActionType.DELETE,
            }
        }
        return relation
