#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class Volume(base.StorPersistentObject, base.StorObject,
             base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'volume_name': fields.StringField(),
        'size': fields.IntegerField(),
        'used': fields.IntegerField(nullable=True),
        'is_link_clone': fields.BooleanField(),
        'snapshot_num': fields.IntegerField(nullable=True),
        'status': s_fields.VolumeStatusField(),
        'display_name': fields.StringField(),
        'display_description': fields.StringField(nullable=True),
        'volume_access_path_id': fields.IntegerField(nullable=True),
        'volume_client_group_id': fields.IntegerField(nullable=True),
        'pool_id': fields.IntegerField(),
        'snapshot_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(),
        'snapshots': fields.ListOfObjectsField("VolumeSnapshot",
                                               nullable=True),
    }

    OPTIONAL_FIELDS = ('snapshots',)

    @property
    def name(self):
        return self.display_name

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume = db.volume_create(self._context, updates)
        self._from_db_object(self._context, self, db_volume)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'snapshots' in expected_attrs:
            snapshots = db_obj.get('snapshots', [])
            obj.snapshots = [objects.VolumeSnapshot._from_db_object(
                context, objects.VolumeSnapshot(context), snapshot
            ) for snapshot in snapshots]

        return super(Volume, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class VolumeList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Volume'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        volumes = db.volume_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Volume,
                                  volumes, expected_attrs=expected_attrs)
