#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class VolumeSnapshot(base.StorPersistentObject, base.StorObject,
                     base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'uuid': fields.StringField(nullable=True),
        'display_name': fields.StringField(),
        'is_protect': fields.BooleanField(),
        'status': fields.IntegerField(nullable=True),
        'display_description': fields.StringField(nullable=True),
        'volume_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(),
    }

    @property
    def name(self):
        return self.display_name

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume = db.volume_snapshot_create(self._context, updates)
        self._from_db_object(self._context, self, db_volume)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_snapshot_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_snapshot_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class VolumeSnapshotList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeSnapshot'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_snapshot = db.volume_snapshot_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        expected_attrs = VolumeSnapshot._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context),
                                  objects.VolumeSnapshot, volume_snapshot,
                                  expected_attrs=expected_attrs)
