#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class Pool(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'display_name': fields.StringField(nullable=False),
        'pool_id': fields.IntegerField(nullable=True),
        'pool_name': fields.StringField(nullable=True),
        'type': fields.StringField(nullable=True),
        'data_chunk_num': fields.IntegerField(nullable=True),
        'coding_chunk_num': fields.IntegerField(nullable=True),
        'replicate_size': fields.IntegerField(nullable=True),
        'role': s_fields.PoolRoleField(nullable=True),
        'status': s_fields.PoolStatusField(nullable=True),
        'size': fields.IntegerField(nullable=True),
        'used': fields.IntegerField(nullable=True),
        'osd_num': fields.IntegerField(nullable=True),
        'speed_type': fields.StringField(nullable=True),
        'failure_domain_type': fields.StringField(nullable=True),
        'crush_rule_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(nullable=True),
        'rgw_zone_id': fields.UUIDField(nullable=True),
        'osds': fields.ListOfObjectsField('Osd', nullable=True),
        'crush_rule': fields.ObjectField("CrushRule", nullable=True),
        'volumes': fields.ListOfObjectsField('Volume', nullable=True),
        'metrics': s_fields.DictOfNullableField(nullable=True),
        'policies': fields.ListOfObjectsField('ObjectPolicy', nullable=True)
    }

    OPTIONAL_FIELDS = ('osds', 'crush_rule', 'volumes', 'metrics',
                       'failure_domain_type', 'policies')

    def need_metrics(self):
        return self.status not in [s_fields.PoolStatus.CREATING,
                                   s_fields.PoolStatus.DELETING,
                                   s_fields.PoolStatus.ERROR,
                                   s_fields.PoolStatus.DELETED]

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_pool = db.pool_create(self._context, updates)
        self._from_db_object(self._context, self, db_pool)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.pool_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.pool_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        obj.metrics = {}
        expected_attrs = expected_attrs or []
        crush_rule = db_obj.get('crush_rule', None)
        if 'crush_rule' in expected_attrs and crush_rule:
            obj.crush_rule = objects.CrushRule._from_db_object(
                context, objects.CrushRule(context), crush_rule
            )
            obj.failure_domain_type = crush_rule.content.get('fault_domain')
        if 'osds' in expected_attrs:
            osds = db_obj.get('osds', [])
            obj.osds = [objects.Osd._from_db_object(
                context, objects.Osd(context), osd
            ) for osd in osds]
        if 'volumes' in expected_attrs:
            volumes = db_obj.get('volumes', [])
            obj.volumes = [objects.Volume._from_db_object(
                context, objects.Volume(context), volume
            ) for volume in volumes]
        if 'policies' in expected_attrs:
            policies = db_obj.get('policies', [])
            obj.policies = [objects.ObjectPolicy._from_db_object(
                context, objects.ObjectPolicy(context), policy
            ) for policy in policies]
        return super(Pool, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class PoolList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Pool'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        pools = db.pool_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Pool,
                                  pools, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.pool_get_count(context, filters)
        return count

    @classmethod
    def get_status(cls, context):
        return db.pool_status_get(context)
