#!/usr/bin/env python
# -*- coding: utf-8 -*-

import oslo_messaging as messaging

from t2stor import exception
from t2stor.context import RequestContext


class RequestContextSerializer(messaging.Serializer):

    def __init__(self, base):
        self._base = base

    def serialize_entity(self, context, entity):
        if not self._base:
            return entity
        return self._base.serialize_entity(context, entity)

    def deserialize_entity(self, context, entity):
        if not self._base:
            return entity
        return self._base.deserialize_entity(context, entity)

    def serialize_exception(self, context, e):
        cls = e.obj_cls() if hasattr(e, "obj_cls") else "StorException"
        _e = {
            "__type__": "Exception",
            "__class__": cls,
            "msg": str(e),
            "code": getattr(e, 'code', 500)
        }
        return _e

    def deserialize_exception(self, context, e):
        if isinstance(e, dict) and e.get('__type__') == "Exception":
            cls_name = e['__class__']
            cls = getattr(exception, cls_name)
            ex = cls(code=e['code'], message=e['msg'])
            raise ex
        return e

    def serialize_context(self, context):
        _context = context.to_dict()
        return _context

    def deserialize_context(self, context):
        return RequestContext.from_dict(context)
