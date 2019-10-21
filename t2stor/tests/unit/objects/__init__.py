#!/usr/bin/env python
# -*- coding: utf-8 -*-


from oslo_utils import timeutils

from t2stor import context
from t2stor import exception
from t2stor import test
from t2stor.objects import base as obj_base


class BaseObjectsTestCase(test.TestCase):
    def setUp(self, *args, **kwargs):
        super(BaseObjectsTestCase, self).setUp(*args, **kwargs)
        self.user_id = 'fake-user'
        self.project_id = 'fake-project'
        self.context = context.RequestContext(self.user_id,
                                              self.project_id,
                                              is_admin=False)
        # We only test local right now.
        # TODO(mriedem): Testing remote would be nice...
        self.assertIsNone(obj_base.StorObject.indirection_api)

    # TODO(mriedem): Replace this with
    # oslo_versionedobjects.fixture.compare_obj when that is in a released
    # version of o.vo.
    @staticmethod
    def _compare(test, db, obj):
        for field, value in db.items():
            try:
                getattr(obj, field)
            except (AttributeError, exception.StorException,
                    NotImplementedError):
                # NotImplementedError: ignore "Cannot load 'projects' in the
                # base class" error
                continue

            obj_field = getattr(obj, field)
            if field in ('modified_at', 'created_at', 'updated_at',
                         'deleted_at', 'last_heartbeat') and db[field]:
                test.assertEqual(db[field],
                                 timeutils.normalize_time(obj_field))
            elif isinstance(obj_field, obj_base.ObjectListBase):
                test.assertEqual(db[field], obj_field.objects)
            else:
                test.assertEqual(db[field], obj_field)
