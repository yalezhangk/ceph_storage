#!/usr/bin/env python
# -*- coding: utf-8 -*-

import oslo_messaging as messaging

from stor.context import RequestContext


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

    def serialize_context(self, context):
        _context = context.to_dict()
        return _context

    def deserialize_context(self, context):
        return RequestContext.from_dict(context)
