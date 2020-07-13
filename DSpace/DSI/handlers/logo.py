#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import six
from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import AnonymousHandler

logger = logging.getLogger(__name__)


logo_schame = {
    "type": "object",
    "properties": {
        "banner": {"type": "string"},
        "icon": {"type": "string"},
        "light": {"type": "string"},
        "logo": {"type": "string"},
    },
}


@URLRegistry.register(r"/logo/")
class LogoListHandler(AnonymousHandler):
    @gen.coroutine
    def get(self):
        """获取logo信息
        ---
        tags:
        - logo
        summary: logo List
        description: Return a list of logos
        operationId: logos.api.listLogo
        produces:
        - application/json
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        logos = objects.LogoList.get_all(ctxt)
        res = {
            "banner": None,
            "icon": None,
            "light": None,
            "logo": None,
        }

        for logo in logos:
            res[logo.name] = logo.data.decode("utf-8")

        self.write(res)

    @gen.coroutine
    def post(self):
        """更新logo

        ---
        tags:
        - logo
        summary: Create logo
        description: create logo.
        operationId: logos.api.createLogo
        produces:
        - application/json
        """
        data = json_decode(self.request.body)
        validate(data, schema=logo_schame,
                 format_checker=draft7_format_checker)
        ctxt = self.get_context()
        for key, val in six.iteritems(data):
            logger.info("Update logo %s", key)
            try:
                logo = objects.Logo.get(ctxt, key)
                logo.data = val.encode('utf-8')
                logo.save()
            except exception.LogoNotFound:
                logo = objects.Logo(ctxt, name=key)
                logo.data = val.encode('utf-8')
                logo.create()
