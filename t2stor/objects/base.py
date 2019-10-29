#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import logging

import netaddr
import six
from oslo_versionedobjects import base
from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.i18n import _

logger = logging.getLogger(__name__)
obj_make_list = base.obj_make_list


class StorObjectRegistry(base.VersionedObjectRegistry):
    def registration_hook(self, cls, index):
        setattr(objects, cls.obj_name(), cls)


class StorObject(base.VersionedObject):
    OBJ_PROJECT_NAMESPACE = 'stor'

    def stor_obj_get_changes(self):
        """Returns a dict of changed fields with tz unaware datetimes.

        Any timezone aware datetime field will be converted to UTC timezone
        and returned as timezone unaware datetime.

        This will allow us to pass these fields directly to a db update
        method as they can't have timezone information.
        """
        # Get dirtied/changed fields
        changes = self.obj_get_changes()

        # Look for datetime objects that contain timezone information
        for k, v in changes.items():
            if isinstance(v, datetime.datetime) and v.tzinfo:
                # Remove timezone information and adjust the time according to
                # the timezone information's offset.
                changes[k] = v.replace(tzinfo=None) - v.utcoffset()
            elif isinstance(v, netaddr.IPAddress):
                changes[k] = str(v)

        # Return modified dict
        return changes


class StorObjectDictCompat(base.VersionedObjectDictCompat):
    pass


class StorPersistentObject(object):
    fields = {
        'created_at': fields.DateTimeField(nullable=True),
        'updated_at': fields.DateTimeField(nullable=True),
        'deleted_at': fields.DateTimeField(nullable=True),
        'deleted': fields.BooleanField(default=False, nullable=True),
    }
    OPTIONAL_FIELDS = ()

    @classmethod
    def _get_expected_attrs(cls, context, *args, **kwargs):
        return None

    @classmethod
    def get_by_id(cls, context, id, expected_attrs=None, *args, **kwargs):
        if 'id' not in cls.fields:
            msg = ('VersionedObject %s cannot retrieve object by id.' %
                   cls.obj_name())
            raise NotImplementedError(msg)

        orm_obj = db.get_by_id(context, cls.obj_name(), id, *args, **kwargs)
        return cls._from_db_object(context, cls(context), orm_obj,
                                   expected_attrs)

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
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

    def obj_load_attr(self, attrname):
        if attrname not in self.OPTIONAL_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr',
                reason=_('attribute %s not lazy-loadable') % attrname)
        setattr(self, attrname, None)

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


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, StorPersistentObject):
            _obj = {}
            for k in six.iterkeys(obj.fields):
                v = getattr(obj, k)
                if isinstance(v, datetime.datetime):
                    v = datetime.datetime.isoformat(v)
                if isinstance(v, netaddr.IPAddress):
                    v = str(v)
                _obj[k] = v
            return _obj

        if isinstance(obj, ObjectListBase):
            return list(obj)

        return super(JsonEncoder, self).default(obj)
