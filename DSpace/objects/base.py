#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

import datetime
import json
import logging

import netaddr
import six
from oslo_versionedobjects import base
from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.i18n import _
from DSpace.utils import utc_to_local

logger = logging.getLogger(__name__)
obj_make_list = base.obj_make_list


class StorObjectRegistry(base.VersionedObjectRegistry):
    def registration_hook(self, cls, index):
        setattr(objects, cls.obj_name(), cls)

        if isinstance(getattr(cls, 'cinder_ovo_cls_init', None),
                      Callable):
            cls.cinder_ovo_cls_init()


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

        for field in self.OPTIONAL_FIELDS:
            changes.pop(field, None)

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
    Not = db.Not
    Case = db.Case

    @classmethod
    def cinder_ovo_cls_init(cls):
        """This method is called on OVO registration and sets the DB model."""
        # Persistent Versioned Objects Classes should have a DB model, and if
        # they don't, then we have a problem and we must raise an exception on
        # registration.
        try:
            cls._db_model = db.get_model_for_versioned_object(cls)
        except (ImportError, AttributeError):
            msg = _("Couldn't find ORM model for Persistent Versioned "
                    "Object %s.") % cls.obj_name()
            logger.exception("Failed to initialize object.")
            raise exception.ProgrammingError(reason=msg)

    @classmethod
    def _get_expected_attrs(cls, context, *args, **kwargs):
        return cls.OPTIONAL_FIELDS

    @classmethod
    def get_by_id(cls, context, id, expected_attrs=None, *args, **kwargs):
        if 'id' not in cls.fields:
            msg = ('VersionedObject %s cannot retrieve object by id.' %
                   cls.obj_name())
            raise NotImplementedError(msg)

        if kwargs.pop('joined_load', False):
            expected_attrs = cls._get_expected_attrs(context)

        if expected_attrs:
            kwargs['expected_attrs'] = expected_attrs
        orm_obj = db.get_by_id(context, cls.obj_name(), id,
                               *args, **kwargs)
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

    def conditional_update(self, values, expected_values=None, filters=(),
                           save_all=False, session=None, reflect_changes=True,
                           order=None):
        """Compare-and-swap update.

        A conditional object update that, unlike normal update, will SAVE the
        contents of the update to the DB.

        Update will only occur in the DB and the object if conditions are met.

        If no expected_values are passed in we will default to make sure that
        all fields have not been changed in the DB. Since we cannot know the
        original value in the DB for dirty fields in the object those will be
        excluded.

        We have 4 different condition types we can use in expected_values:
         - Equality:  {'status': 'available'}
         - Inequality: {'status': vol_obj.Not('deleting')}
         - In range: {'status': ['available', 'error']
         - Not in range: {'status': vol_obj.Not(['in-use', 'attaching'])

        :param values: Dictionary of key-values to update in the DB.
        :param expected_values: Dictionary of conditions that must be met for
                                the update to be executed.
        :param filters: Iterable with additional filters
        :param save_all: Object may have changes that are not in the DB, this
                         will say whether we want those changes saved as well.
        :param session: Session to use for the update
        :param reflect_changes: If we want changes made in the database to be
                                reflected in the versioned object.  This may
                                mean in some cases that we have to reload the
                                object from the database.
        :param order: Specific order of fields in which to update the values
        :returns: number of db rows that were updated, which can be used as a
                  boolean, since it will be 0 if we couldn't update the DB and
                  1 if we could, because we are using unique index id.
        """
        if 'id' not in self.fields:
            msg = (_('VersionedObject %s does not support conditional update.')
                   % (self.obj_name()))
            raise NotImplementedError(msg)

        # If no conditions are set we will require object in DB to be unchanged
        if expected_values is None:
            changes = self.obj_what_changed()

            expected = {key: getattr(self, key)
                        for key in self.fields.keys()
                        if self.obj_attr_is_set(key) and key not in changes and
                        key not in self.OPTIONAL_FIELDS}
        else:
            # Set the id in expected_values to limit conditional update to only
            # change this object
            expected = expected_values.copy()
            expected['id'] = self.id

        # If we want to save any additional changes the object has besides the
        # ones referred in values
        if save_all:
            changes = self.cinder_obj_get_changes()
            changes.update(values)
            values = changes

        result = db.conditional_update(self._context, self._db_model, values,
                                       expected, filters, order=order)

        # If we were able to update the DB then we need to update this object
        # as well to reflect new DB contents and clear the object's dirty flags
        # for those fields.
        if result and reflect_changes:
            # If we have used a Case, a db field or an expression in values we
            # don't know which value was used, so we need to read the object
            # back from the DB
            if any(isinstance(v, self.Case) or db.is_orm_value(v)
                   for v in values.values()):
                # Read back object from DB
                obj = type(self).get_by_id(self._context, self.id)
                db_values = obj.obj_to_primitive()['versioned_object.data']
                # Only update fields were changes were requested
                values = {field: db_values[field]
                          for field, value in values.items()}

            for key, value in values.items():
                setattr(self, key, value)
            self.obj_reset_changes(values.keys())
        return result

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
        return db.resource_exists(context, cls._db_model, id_)


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
            for k, field in six.iteritems(obj.fields):
                v = getattr(obj, k)
                if isinstance(v, datetime.datetime):
                    local_time = utc_to_local(v, CONF.time_zone)
                    v = datetime.datetime.isoformat(local_time)
                elif isinstance(v, netaddr.IPAddress):
                    v = str(v)
                elif isinstance(field, fields.SensitiveStringField):
                    v = "***"
                _obj[k] = v
            return _obj

        if isinstance(obj, ObjectListBase):
            return list(obj)

        return super(JsonEncoder, self).default(obj)
