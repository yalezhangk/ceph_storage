#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_field

logger = logging.getLogger(__name__)


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
        'node': fields.ObjectField("Node", nullable=True),
        'disk': fields.ObjectField("Disk", nullable=True),
    }

    OPTIONAL_FIELDS = ('node', 'disk')

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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'node' in expected_attrs:
            node = db_obj.get('node', None)
            obj.node = objects.Node._from_db_object(
                context, objects.Node(context), node
            )
        if 'disk' in expected_attrs:
            disk = db_obj.get('disk', None)
            obj.disk = objects.Disk._from_db_object(
                context, objects.Disk(context), disk
            )
        return super(DiskPartition, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class DiskPartitionList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('DiskPartition'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        disk_partitions = db.disk_partition_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs,
            expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.DiskPartition,
            disk_partitions, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.disk_partition_get_count(context, filters)
        return count

    @classmethod
    def get_all_available(cls, context, filters=None, expected_attrs=None):
        partitions = db.disk_partition_get_all_available(
            context, filters, expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context),
                                  objects.DiskPartition, partitions,
                                  expected_attrs=expected_attrs)
