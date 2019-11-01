#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


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
        'role': fields.StringField(nullable=True),
        'status': fields.StringField(nullable=True),
        'size': fields.IntegerField(nullable=True),
        'used': fields.IntegerField(nullable=True),
        'osd_num': fields.IntegerField(nullable=True),
        'speed_type': fields.StringField(nullable=True),
        'failure_domain_type': fields.StringField(nullable=True),
        'crush_rule_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.UUIDField(nullable=True),
    }

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


@base.StorObjectRegistry.register
class PoolList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Pool'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        pools = db.pool_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Pool,
                                  pools)
