#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class SysConfig(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'service_id': fields.StringField(),
        'key': fields.StringField(),
        'value': fields.StringField(),
        'value_type': fields.StringField(),
        'display_description': fields.StringField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_node = db.node_create(self._context, updates)
        self._from_db_object(self._context, self, db_node)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.node_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.node_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class NodeList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Node'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        nodes = db.node_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs)
        expected_attrs = Node._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.Node,
                                  nodes, expected_attrs=expected_attrs)
