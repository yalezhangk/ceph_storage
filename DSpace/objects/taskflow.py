#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_utils import timeutils
from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import TaskStatus


@base.StorObjectRegistry.register
class Taskflow(base.StorPersistentObject, base.StorObject,
               base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'description': fields.StringField(nullable=True),
        'status': s_fields.TaskStatusField(
            default=s_fields.TaskStatus.RUNNING),
        'reason': fields.StringField(nullable=True),
        'args': s_fields.DictOfNullableField(nullable=True),
        'finished_at': fields.DateTimeField(nullable=True),
        'action_log_id': fields.IntegerField(nullable=True),
        'enable_redo': fields.BooleanField(),
        'enable_clean': fields.BooleanField(),
        'cluster_id': fields.UUIDField(nullable=True),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_taskflow = db.taskflow_create(self._context, updates)
        self._from_db_object(self._context, self, db_taskflow)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.taskflow_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def finish(self):
        self.status = TaskStatus.SUCCESS
        self.finished_at = timeutils.utcnow()
        self.save()

    def failed(self, reason):
        self.status = TaskStatus.FAILED
        self.reason = reason
        self.finished_at = timeutils.utcnow()
        self.save()

    def destroy(self):
        updated_values = db.taskflow_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class TaskflowList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Taskflow'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        taskflows = db.taskflow_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Taskflow,
                                  taskflows)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.taskflow_get_count(context, filters)
        return count
