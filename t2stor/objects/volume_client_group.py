#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class VolumeClientGroup(base.StorPersistentObject, base.StorObject,
                        base.StorObjectDictCompat, base.StorComparableObject):
    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'type': fields.StringField(nullable=True),
        'chap_enable': fields.BooleanField(),
        'chap_username': fields.StringField(nullable=True),
        'chap_password': fields.StringField(nullable=True),
        'access_path_id': fields.IntegerField(),
        'cluster_id': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_client_group = db.volume_client_group_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_client_group)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_client_group_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_client_group_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class VolumeClientGroupList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeClientGroup'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_client_groups = db.volume_client_group_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        expected_attrs = VolumeClientGroup._get_expected_attrs(context)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeClientGroup,
            volume_client_groups,
            expected_attrs=expected_attrs)