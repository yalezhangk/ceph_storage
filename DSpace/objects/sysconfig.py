#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_utils import strutils
from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.i18n import _
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class SysConfig(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'key': fields.StringField(),
        'value': fields.StringField(),
        'value_type': s_fields.ConfigTypeField(),
        'display_description': fields.StringField(nullable=True),
        'cluster_id': fields.UUIDField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_sys_config = db.sys_config_create(self._context, updates)
        self._from_db_object(self._context, self, db_sys_config)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.sys_config_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.sys_config_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def get_by_key(cls, context, key, cluster_id=None):
        orm_obj = db.sys_config_get_by_key(context, key, cluster_id)
        if not orm_obj:
            return None
        return cls._from_db_object(context, cls(context), orm_obj)


@base.StorObjectRegistry.register
class SysConfigList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('SysConfig'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        sys_configs = db.sys_config_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.SysConfig,
                                  sys_configs)


def sys_config_get(ctxt, key, default=None):
    obj = SysConfig.get_by_key(ctxt, key, cluster_id=ctxt.cluster_id)
    if not obj:
        obj = SysConfig.get_by_key(ctxt, key, cluster_id=None)
    if not obj:
        return default
    if obj.value_type == s_fields.ConfigType.STRING:
        return obj.value
    elif obj.value_type == s_fields.ConfigType.NUMBER:
        return int(obj.value)
    elif obj.value_type == s_fields.ConfigType.BOOL:
        return strutils.bool_from_string(obj.value)
    else:
        raise exception.Invalid(msg=_("Invalid config type"))


def sys_config_set(ctxt, key, value, value_type=None):
    if not value_type:
        if isinstance(value, bool):
            value_type = s_fields.ConfigType.BOOL
        elif isinstance(value, int):
            value_type = s_fields.ConfigType.NUMBER
        else:
            value_type = s_fields.ConfigType.STRING
    objs = SysConfigList.get_all(ctxt, filters={"key": key})
    if objs:
        obj = objs[0]
        obj.value = value
        obj.value_type = value_type
        obj.save()
    else:
        obj = SysConfig(
            ctxt, key=key, value=value, value_type=value_type
        )
        obj.create()
