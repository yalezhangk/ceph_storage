#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class VolumeSnapshot(base.StorPersistentObject, base.StorObject,
                     base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'uuid': fields.StringField(),
        'display_name': fields.StringField(),
        'is_protect': fields.BooleanField(),
        'status': s_fields.VolumeSnapshotStatusField(),
        'display_description': fields.StringField(nullable=True),
        'volume_id': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
        'volume': fields.ObjectField('Volume', nullable=True),
        'pool': fields.ObjectField('Pool', nullable=True),
        'child_volumes': fields.ListOfObjectsField('Volume', nullable=True)
    }

    OPTIONAL_FIELDS = ('volume', 'pool', 'child_volumes')

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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'volume' in expected_attrs:
            volume = db_obj.get('volume', None)
            if volume:
                obj.volume = objects.Volume._from_db_object(
                    context, objects.Volume(context), volume)
            else:
                obj.volume = None

        if 'pool' in expected_attrs:
            pool = db_obj.get('pool', None)
            if pool:
                obj.pool = objects.Pool._from_db_object(
                    context, objects.Pool(context), pool)
            else:
                obj.pool = None

        if 'child_volumes' in expected_attrs:
            child_volumes = db_obj.get('child_volumes', [])
            obj.child_volumes = [objects.Volume._from_db_object(
                context, objects.Volume(context), child_volume
            ) for child_volume in child_volumes]

        return super(VolumeSnapshot, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class VolumeSnapshotList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeSnapshot'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        volume_snapshot = db.volume_snapshot_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context),
                                  objects.VolumeSnapshot, volume_snapshot,
                                  expected_attrs=expected_attrs)
