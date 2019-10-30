#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base
from t2stor.objects import fields as s_fields

logger = logging.getLogger(__name__)


@base.StorObjectRegistry.register
class Osd(base.StorPersistentObject, base.StorObject,
          base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'osd_id': fields.StringField(),
        'size': fields.IntegerField(),
        'used': fields.IntegerField(),
        'status': s_fields.OsdStatusField(),
        'type': s_fields.OsdTypeField(),
        'disk_type': s_fields.OsdDiskTypeField(),
        'fsid': fields.StringField(nullable=True),
        'mem_read_cache': fields.IntegerField(),
        'node_id': fields.IntegerField(),
        'disk_id': fields.IntegerField(),
        'cache_partition_id': fields.IntegerField(),
        'db_partition_id': fields.IntegerField(),
        'wal_partition_id': fields.IntegerField(),
        'journal_partition_id': fields.IntegerField(),
        'crush_rule_id': fields.IntegerField(),
        'cluster_id': fields.UUIDField(nullable=True),
        'node': fields.ObjectField("Node", nullable=True),
        'disk': fields.ObjectField("Disk", nullable=True),
        'cache_partition': fields.ObjectField("DiskPartition", nullable=True),
        'db_partition': fields.ObjectField("DiskPartition", nullable=True),
        'wal_partition': fields.ObjectField("DiskPartition", nullable=True),
        'journal_partition': fields.ObjectField("DiskPartition",
                                                nullable=True),
    }

    OPTIONAL_FIELDS = ('node', 'disk')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_osd = db.osd_create(self._context, updates)
        self._from_db_object(self._context, self, db_osd)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.osd_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.osd_destroy(self._context, self.id)
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
        partations = ["cache_partition", "db_partition",
                      "wal_partition", "journal_partition"]
        for attr in partations:
            logger.debug("try load %s", attr)
            if attr in expected_attrs:
                db_partition = db_obj.get(attr, None)
                obj_partition = objects.DiskPartition._from_db_object(
                    context, objects.DiskPartition(context), db_partition
                )
                setattr(obj, attr, obj_partition)
        return super(Osd, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class OsdList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Osd'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        osds = db.osd_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Osd,
                                  osds, expected_attrs=expected_attrs)

    @classmethod
    def get_by_pool(cls, context, pool_id):
        db_osds = db.osd_get_by_pool(context, pool_id)
        return base.obj_make_list(context, cls(context), objects.Osd, db_osds)
