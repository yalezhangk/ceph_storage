#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_fields


@base.StorObjectRegistry.register
class Network(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'status': s_fields.NetworkStatusField(),
        'ip_address': fields.IPAddressField(nullable=True),
        'netmask': fields.StringField(nullable=True),
        'mac_address': fields.StringField(nullable=True),
        'type': s_fields.NetworkTypeField(),
        'speed': fields.StringField(),
        'node_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
        'node': fields.ObjectField("Node", nullable=True)
    }

    OPTIONAL_FIELDS = ('node',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_net = db.network_create(self._context, updates)
        self._from_db_object(self._context, self, db_net)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.network_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.network_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'node' in expected_attrs:
            node = db_obj.get('node', None)
            obj.node = objects.Node._from_db_object(
                context, objects.Node(context), node
            )
        return super(Network, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class NetworkList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Network'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        nets = db.network_get_all(context, filters, marker, limit, offset,
                                  sort_keys, sort_dirs,
                                  expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Network,
                                  nets, expected_attrs=expected_attrs)
