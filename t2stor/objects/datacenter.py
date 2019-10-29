#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class Datacenter(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'cluster_id': fields.UUIDField()
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_datacenter = db.datacenter_create(self._context, updates)
        self._from_db_object(self._context, self, db_datacenter)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.datacenter_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.datacenter_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class DatacenterList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Datacenter'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        datacenters = db.datacenter_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Datacenter,
                                  datacenters)
