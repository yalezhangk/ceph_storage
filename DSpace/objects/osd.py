#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


@base.StorObjectRegistry.register
class Osd(base.StorPersistentObject, base.StorObject,
          base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'osd_id': fields.StringField(nullable=True),
        'size': fields.IntegerField(),
        'used': fields.IntegerField(),
        'status': s_fields.OsdStatusField(),
        'type': s_fields.OsdTypeField(),
        'disk_type': s_fields.OsdDiskTypeField(),
        'fsid': fields.StringField(nullable=True),
        'mem_read_cache': fields.IntegerField(),
        'node_id': fields.IntegerField(),
        'disk_id': fields.IntegerField(),
        'cache_partition_id': fields.IntegerField(nullable=True),
        'db_partition_id': fields.IntegerField(nullable=True),
        'wal_partition_id': fields.IntegerField(nullable=True),
        'journal_partition_id': fields.IntegerField(nullable=True),
        'crush_rule_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(nullable=True),
        'node': fields.ObjectField("Node", nullable=True),
        'disk': fields.ObjectField("Disk", nullable=True),
        'pools': fields.ListOfObjectsField('Pool', nullable=True),
        'cache_partition': fields.ObjectField("DiskPartition", nullable=True),
        'db_partition': fields.ObjectField("DiskPartition", nullable=True),
        'wal_partition': fields.ObjectField("DiskPartition", nullable=True),
        'journal_partition': fields.ObjectField("DiskPartition",
                                                nullable=True),
        'metrics': s_fields.DictOfNullableField(nullable=True),
    }

    OPTIONAL_FIELDS = ('node', 'disk', 'pools', 'cache_partition',
                       'db_partition', 'wal_partition', 'journal_partition',
                       'metrics')

    @property
    def osd_name(self):
        if not self.osd_id:
            return None
        return "osd.%s" % self.osd_id

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

    def need_size(self):
        if self.status in [s_fields.OsdStatus.ACTIVE,
                           s_fields.OsdStatus.WARNING]:
            return True
        return False

    @classmethod
    def get_by_osd_id(cls, context, osd_id, expected_attrs=None):
        kwargs = {}
        if expected_attrs:
            kwargs['expected_attrs'] = expected_attrs
        db_osd = db.osd_get_by_osd_id(context, osd_id, **kwargs)
        return cls._from_db_object(context, cls(context), db_osd, **kwargs)

    @classmethod
    def get_by_osd_name(cls, context, osd_name, expected_attrs=None):
        osd_id = osd_name.replace("osd.", "")
        return cls.get_by_osd_id(context, osd_id,
                                 expected_attrs=expected_attrs)

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        obj.metrics = {}
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
        if 'pools' in expected_attrs:
            pools = db_obj.get('pools', [])
            obj.pools = [objects.Pool._from_db_object(
                context, objects.Pool(context), pool
            ) for pool in pools]
        partations = ["cache_partition", "db_partition",
                      "wal_partition", "journal_partition"]
        for attr in partations:
            if attr in expected_attrs:
                db_partition = db_obj.get(attr, None)
                if not db_partition:
                    continue
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
    def get_count(cls, context, filters=None):
        count = db.osd_get_count(context, filters)
        return count

    @classmethod
    def get_by_pool(cls, context, pool_id, expected_attrs=None):
        db_osds = db.osd_get_by_pool(context, pool_id,
                                     expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.Osd, db_osds,
            expected_attrs=expected_attrs)

    @classmethod
    def get_status(cls, context):
        return db.osd_status_get(context)
