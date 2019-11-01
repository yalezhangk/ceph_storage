#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_field


@base.StorObjectRegistry.register
class DiskPartition(base.StorPersistentObject, base.StorObject,
                    base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'size': fields.IntegerField(),
        'status': s_field.DiskPartitionStatusField(),
        'type': s_field.DiskPartitionTypeField(),
        'role': s_field.DiskPartitionRoleField(nullable=True),
        'node_id': fields.IntegerField(),
        'disk_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_disk_partition = db.disk_partition_create(self._context, updates)
        self._from_db_object(self._context, self, db_disk_partition)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.disk_partition_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.disk_partition_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class DiskPartitionList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('DiskPartition'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        disk_partitions = db.disk_partition_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs)
        return base.obj_make_list(
            context, cls(context), objects.DiskPartition,
            disk_partitions)
