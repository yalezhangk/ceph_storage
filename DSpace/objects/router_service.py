#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class RouterService(base.StorPersistentObject, base.StorObject,
                    base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'status': s_fields.RouterServiceStatusField(nullable=True),
        'node_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
        'net_id': fields.IntegerField(),
        'router_id': fields.IntegerField(),
        'counter': fields.IntegerField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_router_service = db.router_service_create(self._context, updates)
        self._from_db_object(self._context, self, db_router_service)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.router_service_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.router_service_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class RouterServiceList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('RouterService'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        router_services = db.router_service_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.RouterService,
                                  router_services)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.router_service_get_count(context, filters)
        return count
