#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class VolumeAccessPath(base.StorPersistentObject, base.StorObject,
                       base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),

        'name': fields.StringField(nullable=True),
        'iqn': fields.StringField(nullable=True),
        'status': s_fields.VolumeAccessPathStatusField(nullable=True),
        'type': fields.StringField(nullable=True),
        'chap_enable': fields.BooleanField(default=False),
        'chap_username': fields.StringField(nullable=True),
        'chap_password': fields.StringField(nullable=True),
        'cluster_id': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_access_path = db.volume_access_path_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_access_path)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_access_path_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_access_path_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class VolumeAccessPathList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeAccessPath'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_access_paths = db.volume_access_path_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeAccessPath,
            volume_access_paths)

    # TODO Get all APGateways by access_path_id
