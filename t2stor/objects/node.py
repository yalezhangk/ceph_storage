#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class Node(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'hostname': fields.StringField(nullable=True),
        'ip_address': fields.IPAddressField(),
        'gateway_ip_address': fields.IPAddressField(nullable=True),
        'storage_cluster_ip_address': fields.IPAddressField(),
        'storage_public_ip_address': fields.IPAddressField(),
        'password': fields.SensitiveStringField(nullable=True),
        'status': s_fields.NodeStatusField(),
        'role_base': fields.BooleanField(),
        'role_admin': fields.BooleanField(),
        'role_monitor': fields.BooleanField(),
        'role_storage': fields.BooleanField(),
        'role_block_gateway': fields.BooleanField(),
        'role_object_gateway': fields.BooleanField(),
        'vendor': fields.StringField(nullable=True),
        'model': fields.StringField(nullable=True),
        'cpu_num': fields.IntegerField(nullable=True),
        'cpu_model': fields.StringField(nullable=True),
        'cpu_core_num': fields.IntegerField(nullable=True),
        'mem_num': fields.IntegerField(nullable=True),
        'sys_type': fields.StringField(nullable=True),
        'sys_version': fields.StringField(nullable=True),
        'rack_id': fields.IntegerField(nullable=True),
        'time_diff': fields.IntegerField(nullable=True),
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
