#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class Rack(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'datacenter_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
        'nodes': fields.ListOfObjectsField("Node", nullable=True)
    }

    OPTIONAL_FIELDS = ('nodes', )

    def create(self):
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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if "nodes" in expected_attrs:
            nodes = db_obj.get('nodes', [])
            obj.nodes = [objects.Node._from_db_object(
                context, objects.Node(context), node
            ) for node in nodes]

        return super(Rack, cls)._from_db_object(context, obj, db_obj)


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
        return base.obj_make_list(context, cls(context), objects.Rack,
                                  racks)
