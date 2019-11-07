#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


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
        'volume_gateways': fields.ListOfObjectsField(
            'VolumeGateway', nullable=True),
        'volume_client_groups': fields.ListOfObjectsField(
            'VolumeClientGroup', nullable=True),
    }

    OPTIONAL_FIELDS = ('volume_gateways', 'volume_client_groups')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_access_path = db.volume_access_path_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_access_path)

    def volume_gateway_append(self, volume_gateway_id):
        db.volume_access_path_append_gateway(self._context, self.id,
                                             volume_gateway_id)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_access_path_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_access_path_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'volume_gateways' in expected_attrs:
            volume_gateways = db_obj.get('volume_gateways', [])
            obj.volume_gateways = [objects.VolumeGateway._from_db_object(
                context, objects.VolumeGateway(context), volume_gateway
            ) for volume_gateway in volume_gateways]
        if 'volume_client_groups' in expected_attrs:
            volume_client_groups = db_obj.get('volume_client_groups', [])
            obj.volume_client_groups = [
                objects.VolumeClientGroup._from_db_object(
                    context, objects.VolumeClientGroup(context),
                    volume_client_group
                ) for volume_client_group in volume_client_groups]
        return super(VolumeAccessPath, cls)._from_db_object(
            context, obj, db_obj)


@base.StorObjectRegistry.register
class VolumeAccessPathList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeAccessPath'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        volume_access_paths = db.volume_access_path_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset,
            expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeAccessPath,
            volume_access_paths,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.volume_access_path_get_count(context, filters)
        return count

    # TODO Get all APGateways by access_path_id
