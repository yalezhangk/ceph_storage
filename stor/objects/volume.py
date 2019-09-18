#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from stor import db
from stor import exception
from stor import objects
from stor.objects import base
from stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class Volume(base.StorPersistentObject, base.StorObject,
             base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.UUIDField(),

        'status': s_fields.VolumeStatusField(nullable=True),
        'size': fields.IntegerField(nullable=True),

        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),

    }

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


@base.StorObjectRegistry.register
class VolumeList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Volume'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volumes = db.volume_get_all(context, filters, marker, limit, offset,
                                    sort_keys, sort_dirs)
        expected_attrs = Volume._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.Volume,
                                  volumes, expected_attrs=expected_attrs)

    @classmethod
    def get_all_by_pool(cls, context, pool):
        volumes = db.volume_get_all_by_volume(context, pool)
        expected_attrs = Volume._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.Volume,
                                  volumes, expected_attrs=expected_attrs)
