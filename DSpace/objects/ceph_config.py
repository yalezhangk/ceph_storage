#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
from io import StringIO

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class CephConfig(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'group': fields.StringField(),
        'key': fields.StringField(),
        'value': fields.StringField(),
        'value_type': fields.StringField(),
        'display_description': fields.StringField(nullable=True),
        'cluster_id': fields.StringField(nullable=True)
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_ceph_config = db.ceph_config_create(self._context, updates)
        self._from_db_object(self._context, self, db_ceph_config)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.ceph_config_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.ceph_config_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def get_by_key(cls, ctxt, group, key):
        db_obj = db.ceph_config_get_by_key(ctxt, group, key)
        return cls._from_db_object(ctxt, cls(ctxt), db_obj)


@base.StorObjectRegistry.register
class CephConfigList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('CephConfig'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        ceph_configs = db.ceph_config_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.CephConfig,
                                  ceph_configs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.ceph_config_get_count(context, filters)
        return count


def ceph_config_content(ctxt):
    configer = configparser.ConfigParser()
    configs = objects.CephConfigList.get_all(ctxt)
    for config in configs:
        if not configer.has_section(config.group):
            configer[config.group] = {}
        configer[config.group][config.key] = config.value
    buf = StringIO()
    configer.write(buf)
    return buf.getvalue()
