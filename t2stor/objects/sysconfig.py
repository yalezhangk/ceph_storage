#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.i18n import _
from t2stor.objects import base
from t2stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class SysConfig(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'key': fields.StringField(),
        'value': fields.StringField(),
        'value_type': s_fields.SysConfigTypeField(),
        'display_description': fields.StringField(nullable=True),
        'cluster_id': fields.StringField()
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

    def get_by_key(self, key):
        return db.sys_config_get_by_key(key)


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
        expected_attrs = SysConfig._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.SysConfig,
                                  sys_configs, expected_attrs=expected_attrs)


def sys_config_get(ctxt, key, default=None):
    obj = SysConfig.get_by_key(key)
    if not obj:
        return default
    if obj.value_type == s_fields.SysConfigType.STRING:
        return obj.value
    elif obj.value_type == s_fields.SysConfigType.NUMBER:
        return int(obj.value)
    elif obj.value_type == s_fields.SysConfigType.BOOL:
        return bool(obj.value)
    else:
        raise exception.Invalid(msg=_("Invalid config type"))
