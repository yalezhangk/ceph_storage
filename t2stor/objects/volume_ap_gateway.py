#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class VolumeAPGateway(base.StorPersistentObject, base.StorObject,
                      base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'iqn': fields.StringField(nullable=True),
        'node_id': fields.IntegerField(),
        'volume_access_path_id': fields.IntegerField(),
        'cluster_id': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_ap_gateway = db.volume_ap_gateway_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_ap_gateway)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_ap_gateway_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_ap_gateway_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class VolumeAPGatewayList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeAPGateway'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_ap_gateways = db.volume_ap_gateway_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        expected_attrs = VolumeAPGateway._get_expected_attrs(context)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeAPGateway,
            volume_ap_gateways,
            expected_attrs=expected_attrs)
