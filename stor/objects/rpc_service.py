#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from stor import db
from stor import exception
from stor import objects
from stor.objects import base


@base.StorObjectRegistry.register
class RPCService(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(nullable=True),
        'service_name': fields.StringField(nullable=True),
        'hostname': fields.StringField(nullable=True),
        'cluster_id': fields.StringField(nullable=True),
        'endpoint': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_rpc_service = db.rpc_service_create(self._context, updates)
        self._from_db_object(self._context, self, db_rpc_service)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.rpc_service_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.rpc_service_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class RPCServiceList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('RPCService'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        rpc_services = db.rpc_service_get_all(context, filters, marker, limit,
                                              offset, sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.RPCService,
                                  rpc_services)
