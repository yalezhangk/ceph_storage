#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class VolumeClientGroup(base.StorPersistentObject, base.StorObject,
                        base.StorObjectDictCompat, base.StorComparableObject):
    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'type': fields.StringField(nullable=True),
        'chap_enable': fields.BooleanField(default=False),
        'chap_username': fields.StringField(nullable=True),
        'chap_password': fields.StringField(nullable=True),
        'volume_access_path_id': fields.IntegerField(),
        'cluster_id': fields.StringField(nullable=True),
        'volume_access_path': fields.ObjectField(
            "VolumeAccessPath", nullable=True),
        'volumes': fields.ListOfObjectsField('Volume', nullable=True),
        'volume_clients': fields.ListOfObjectsField(
            'VolumeClient', nullable=True),
    }

    OPTIONAL_FIELDS = ('volume_access_path', 'volumes', 'volume_clients')

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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'volume_access_path' in expected_attrs:
            vap = db_obj.get('volume_access_path', None)
            obj.volume_access_path = objects.VolumeAccessPath._from_db_object(
                context, objects.VolumeAccessPath(context), vap
            ) if vap else None
        if 'volume_clients' in expected_attrs:
            volume_clients = db_obj.get('volume_clients', [])
            obj.volume_clients = [objects.VolumeClient._from_db_object(
                context, objects.VolumeClient(context), volume_client
            ) for volume_client in volume_clients]
        if 'volumes' in expected_attrs:
            volumes = db_obj.get('volumes', [])
            obj.volumes = [objects.Volume._from_db_object(
                context, objects.Volume(context), volume
            ) for volume in volumes]
        return super(VolumeClientGroup, cls)._from_db_object(
            context, obj, db_obj)


@base.StorObjectRegistry.register
class VolumeClientGroupList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeClientGroup'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        volume_client_groups = db.volume_client_group_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeClientGroup,
            volume_client_groups, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.volume_client_group_get_count(context, filters)
        return count
