#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import objects
from DSpace.objects import base
from DSpace.objects.fields import LargeBinaryField


@base.StorObjectRegistry.register
class Logo(base.StorPersistentObject, base.StorObject,
           base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'data': LargeBinaryField(),
    }

    def create(self):
        updates = self.stor_obj_get_changes()

        db_logo = db.logo_create(self._context, updates)
        self._from_db_object(self._context, self, db_logo)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.logo_update(self._context, self.name, updates)

        self.obj_reset_changes()

    @classmethod
    def get(cls, context, name):
        orm_obj = db.logo_get(context, name)
        return cls._from_db_object(context, cls(context), orm_obj)


@base.StorObjectRegistry.register
class LogoList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Logo'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        logos = db.logo_get_all(
            context, filters, marker, limit, offset,
            sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Logo,
                                  logos)
