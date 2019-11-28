#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.utils.security import encrypt_password

logger = logging.getLogger(__name__)


@base.StorObjectRegistry.register
class User(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'password': fields.SensitiveStringField(),
        'current_cluster_id': fields.UUIDField(nullable=True)
    }

    def _encrypt_password(self, updates):
        if 'password' in updates:
            updates['password'] = encrypt_password(updates['password'])

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()
        self._encrypt_password(updates)

        db_user = db.user_create(self._context, updates)
        self._from_db_object(self._context, self, db_user)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            self._encrypt_password(updates)
            db.user_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.user_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class UserList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('User'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        users = db.user_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.User,
                                  users, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.user_get_count(context, filters)
        return count
