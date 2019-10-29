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
        'object_gateway_ip_address': fields.IPAddressField(nullable=True),
        'block_gateway_ip_address': fields.IPAddressField(nullable=True),
        'file_gateway_ip_address': fields.IPAddressField(nullable=True),
        'storage_cluster_ip_address': fields.IPAddressField(),
        'storage_public_ip_address': fields.IPAddressField(),
        'password': fields.SensitiveStringField(nullable=True),
        'status': s_fields.NodeStatusField(),
        'role_admin': fields.BooleanField(),
        'role_monitor': fields.BooleanField(),
        'role_storage': fields.BooleanField(),
        'role_block_gateway': fields.BooleanField(),
        'role_object_gateway': fields.BooleanField(),
        'role_file_gateway': fields.BooleanField(),
        'vendor': fields.StringField(nullable=True),
        'model': fields.StringField(nullable=True),
        'cpu_num': fields.IntegerField(nullable=True),
        'cpu_model': fields.StringField(nullable=True),
        'cpu_core_num': fields.IntegerField(nullable=True),
        'mem_size': fields.IntegerField(nullable=True),
        'sys_type': fields.StringField(nullable=True),
        'sys_version': fields.StringField(nullable=True),
        'rack_id': fields.IntegerField(nullable=True),
        'time_diff': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(),
        'disks': fields.ListOfObjectsField("Disk", nullable=True),
        'networks': fields.ListOfObjectsField("Network", nullable=True),
        'osds': fields.ListOfObjectsField("Osd", nullable=True),
    }

    OPTIONAL_FIELDS = ('disks', 'networks', 'osds')

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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'disks' in expected_attrs:
            disks = db_obj.get('disks', [])
            obj.disks = [objects.Disk._from_db_object(
                context, objects.Disk(context), disk
            ) for disk in disks]

        if 'networks' in expected_attrs:
            nets = db_obj.get('networks', [])
            obj.networks = [objects.Network._from_db_object(
                context, objects.Network(context), net
            ) for net in nets]

        if 'osds' in expected_attrs:
            osds = db_obj.get('osds', [])
            obj.osds = [objects.Osd._from_db_object(
                context, objects.Osd(context), osd
            ) for osd in osds]

        return super(Node, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class NodeList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Node'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        nodes = db.node_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs,
                                expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Node,
                                  nodes, expected_attrs=expected_attrs)
