#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class Service(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'status': s_fields.ServiceStatusField(),
        'node_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_service = db.service_create(self._context, updates)
        self._from_db_object(self._context, self, db_service)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.service_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.service_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class ServiceList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Service'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        services = db.service_get_all(context, filters, marker, limit, offset,
                                      sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Service,
                                  services)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.service_get_count(context, filters)
        return count

    @classmethod
    def service_status_get(cls, context, names):
        status = db.service_status_get(context, names=names)
        return status
