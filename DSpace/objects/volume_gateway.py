#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class VolumeGateway(base.StorPersistentObject, base.StorObject,
                    base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'node_id': fields.IntegerField(),
        'volume_access_path_id': fields.IntegerField(),
        'cluster_id': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_gateway = db.volume_gateway_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_gateway)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_gateway_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_gateway_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class VolumeGatewayList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeGateway'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_gateways = db.volume_gateway_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeGateway,
            volume_gateways)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.volume_gateway_get_count(context, filters)
        return count
