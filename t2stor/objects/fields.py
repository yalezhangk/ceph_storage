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

    ALL = (CREATING, AVAILABLE, DELETING, ERROR, ERROR_DELETING,
           ERROR_MANAGING, MANAGING, ATTACHING, IN_USE, DETACHING,
           MAINTENANCE, RESTORING_BACKUP, ERROR_RESTORING,
           RESERVED, AWAITING_TRANSFER, BACKING_UP,
           ERROR_BACKING_UP, ERROR_EXTENDING, DOWNLOADING,
           UPLOADING, RETYPING, EXTENDING)


class VolumeStatusField(BaseEnumField):
    AUTO_TYPE = VolumeStatus()


class NodeStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVATE = 'activate'
    DELETING = 'deleting'
    ERROR = 'error'
    DEPLOYING = 'deploying'

    ALL = (CREATING, ACTIVATE, DELETING, ERROR, DEPLOYING)


class NodeStatusField(BaseEnumField):
    AUTO_TYPE = NodeStatus()


class VolumeAccessPathStatus(BaseStorEnum):
    CREATING = 'creating'
    ACTIVE = 'active'
    DELETING = 'deleting'
    ERROR = 'error'

    ALL = (CREATING, ACTIVE, DELETING, ERROR)


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
