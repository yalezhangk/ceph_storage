#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class VolumeClient(base.StorPersistentObject, base.StorObject,
                   base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'iqn': fields.StringField(nullable=True),
        'client_type': fields.StringField(nullable=True),
        'volume_client_group_id': fields.IntegerField(),
        'cluster_id': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_volume_client = db.volume_client_create(
            self._context, updates)
        self._from_db_object(self._context, self, db_volume_client)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.volume_client_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.volume_client_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def get_by_iqn(cls, ctxt, iqn):
        db_obj = db.volume_client_get_by_iqn(ctxt, iqn)
        if not db_obj:
            return None
        return cls._from_db_object(ctxt, cls(ctxt), db_obj)


@base.StorObjectRegistry.register
class VolumeClientList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('VolumeClient'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        volume_clients = db.volume_client_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return base.obj_make_list(
            context, cls(context),
            objects.VolumeClient,
            volume_clients)
