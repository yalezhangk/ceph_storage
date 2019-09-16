#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from oslo_versionedobjects import base
from oslo_versionedobjects import fields

from stor import db
from stor import exceptions


logger = logging.getLogger(__name__)


class StorObject(base.VersionedObject):
    OBJ_PROJECT_NAMESPACE = 'stor'


class StorObjectDictCompat(base.VersionedObjectDictCompat):
    pass


class CinderPersistentObject(object):
    fields = {
        'created_at': fields.DateTimeField(nullable=True),
        'updated_at': fields.DateTimeField(nullable=True),
        'deleted_at': fields.DateTimeField(nullable=True),
        'deleted': fields.BooleanField(default=False, nullable=True),
    }
    OPTIONAL_FIELDS = ()

    @classmethod
    def cinder_ovo_cls_init(cls):
        try:
            cls.model = db.get_model_for_versioned_object(cls)
        except (ImportError, AttributeError):
            msg = ("Couldn't find ORM model for Persistent Versioned "
                   "Object %s.") % cls.obj_name()
            logger.exception("Failed to initialize object.")
            raise exceptions.ProgrammingError(reason=msg)

    @classmethod
    def _get_expected_attrs(cls, context, *args, **kwargs):
        return None

    @classmethod
    def get_by_id(cls, context, id, *args, **kwargs):
        if 'id' not in cls.fields:
            msg = ('VersionedObject %s cannot retrieve object by id.' %
                   cls.obj_name())
            raise NotImplementedError(msg)

        orm_obj = db.get_by_id(context, cls.model, id, *args, **kwargs)
        # We pass parameters because fields to expect may depend on them
        expected_attrs = cls._get_expected_attrs(context, *args, **kwargs)
        kargs = {}
        if expected_attrs:
            kargs = {'expected_attrs': expected_attrs}
        return cls._from_db_object(context, cls(context), orm_obj, **kargs)

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for name, field in obj.fields.items():
            if name in cls.OPTIONAL_FIELDS:
                continue
            value = db_obj.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            obj[name] = value

        obj._context = context
        obj.obj_reset_changes()
        return obj

    def refresh(self):
        # To refresh we need to have a model and for the model to have an id
        # field
        if 'id' not in self.fields:
            msg = ('VersionedObject %s cannot retrieve object by id.' %
                   self.obj_name())
            raise NotImplementedError(msg)

        current = self.get_by_id(self._context, self.id)

        # Copy contents retrieved from the DB into self
        my_data = vars(self)
        my_data.clear()
        my_data.update(vars(current))

    @classmethod
    def exists(cls, context, id_):
        return db.resource_exists(context, cls.model, id_)


class StorComparableObject(base.ComparableVersionedObject):
    def __eq__(self, obj):
        if hasattr(obj, 'obj_to_primitive'):
            return self.obj_to_primitive() == obj.obj_to_primitive()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class ObjectListBase(base.ObjectListBase):
    pass


class StorObjectSerializer(base.VersionedObjectSerializer):
    OBJ_BASE_CLASS = StorObject
