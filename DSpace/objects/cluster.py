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
class Cluster(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.UUIDField(),

        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),
        'is_admin': fields.BooleanField(),
        'status': s_fields.ClusterStatusField(),

    }

    @property
    def name(self):
        return self.display_name

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_cluster = db.cluster_create(self._context, updates)
        self._from_db_object(self._context, self, db_cluster)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.cluster_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.cluster_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class ClusterList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Cluster'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        clusters = db.cluster_get_all(context, filters, marker, limit, offset,
                                      sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Cluster,
                                  clusters)
