#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class Rack(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'datacenter_id': fields.IntegerField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_rack = db.rack_create(self._context, updates)
        self._from_db_object(self._context, self, db_rack)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.rack_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.rack_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class RackList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Rack'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        racks = db.rack_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        expected_attrs = Rack._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.Rack,
                                  racks, expected_attrs=expected_attrs)
