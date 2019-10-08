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
