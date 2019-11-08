#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_field


@base.StorObjectRegistry.register
class Disk(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'status': s_field.DiskStatusField(),
        'type': s_field.DiskTypeField(),
        'disk_size': fields.IntegerField(),
        'rotate_speed': fields.IntegerField(nullable=True),
        'slot': fields.StringField(nullable=True),
        'model': fields.StringField(nullable=True),
        'vendor': fields.IntegerField(nullable=True),
        'support_led': fields.BooleanField(),
        'led': fields.StringField(nullable=True),
        'has_patrol': fields.BooleanField(nullable=True),
        'patrol_data': fields.StringField(nullable=True),
        'residual_life': fields.IntegerField(nullable=True),
        'role': s_field.DiskRoleField(),
        'partition_num': fields.IntegerField(nullable=True),
        'node_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
        'node': fields.ObjectField("Node", nullable=True),
        'partition_used': fields.IntegerField(nullable=True),
        'metrics': s_field.DictOfNullableField(nullable=True),
    }

    OPTIONAL_FIELDS = ('node', 'partition_used', 'metrics')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_disk = db.disk_create(self._context, updates)
        self._from_db_object(self._context, self, db_disk)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.disk_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.disk_destroy(self._context, self.id)
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
        if 'partition_used' in expected_attrs:
            obj.partition_used = db_obj.get('partition_used', None)
        return super(Disk, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class DiskList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Disk'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        disks = db.disk_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs,
                                expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Disk,
                                  disks, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.disk_get_count(context, filters)
        return count
