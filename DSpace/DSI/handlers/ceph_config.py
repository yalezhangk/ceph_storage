#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)

update_ceph_config_schema = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["config_update", "config_reset"]
        }
    },
    "allOf": [
        {
            "if": {
                "properties": {"action": {"const": "config_update"}}
            },
            "then": {
                "properties": {"ceph_config": {
                    "type": "object",
                    "properties": {
                        "group": {"type": "string"},
                        "key": {"type": "string"},
                        "value": {"type": ["string", "integer",
                                           "boolean", "number"]},
                        "value_type": {"emum": ["int", "string",
                                                "bool", "float"]}
                    },
                    "required": ["group", "key", "value"],
                }}
            }
        }, {
            "if": {
                "properties": {"action": {"const": "config_reset"}}
            },
            "then": {
                "properties": {"ceph_config": {
                    "type": "object",
                    "properties": {
                        "group": {"type": "string"},
                        "key": {"type": "string"},
                    },
                    "required": ["group", "key"],
                    "additionalProperties": False
                }}
            }
        },
    ], "required": ["action", "ceph_config"]
}


@URLRegistry.register(r"/ceph_configs/")
class CephConfigListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - ceph_config
        summary: ceph_config List
        description: Return a list of ceph_config
        operationId: ceph_config.api.listceph_config
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: request
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: group
          type: string
          description: ceph config file [group]
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        value = self.get_query_argument('group', default=None)
        if value:
            filters.update({
                'group': value
            })

        client = self.get_admin_client(ctxt)
        ceph_config, revisable_config = yield client.ceph_config_get_all(
            ctxt, filters=filters, **page_args)
        ceph_config_count = yield client.ceph_config_get_count(
            ctxt, filters=filters)
        self.write(objects.json_encode({
            "ceph_configs": ceph_config,
            "total": ceph_config_count,
            "revisable_config": revisable_config,
        }))


@URLRegistry.register(r"/ceph_configs/([0-9]*)/")
class CephConfigHandler(ClusterAPIHandler):
    @gen.coroutine
    def delete(self, config_id):
        """
        ---
        tags:
        - ceph_config
        summary: Delete the ceph config by id
        description: delete ceph config by id
        operationId: ceph_configs.api.deleteCephConfig
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: ceph_config's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        conf = yield client.ceph_config_remove(ctxt, config_id)
        self.write(objects.json_encode({
            "conf": conf
        }))


@URLRegistry.register(r"/ceph_configs/action/")
class CephConfigActionHandler(ClusterAPIHandler):
    def _ceph_config_set(self, ctxt, client, action, values):
        if action == 'update':
            required_args = ['group', 'key', 'value', 'value_type']

        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("Ceph config: missing required arguments!"))

        return client.ceph_config_set(ctxt, values=values)

    def _ceph_config_update(self, ctxt, client, values):
        return self._ceph_config_set(ctxt, client, 'update', values)

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - ceph_config
        summary: Create ceph_config
        description: Create ceph_config.
        operationId: ceph_configs.api.createCeph_config
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: ceph_config
          description: Created ceph_config object
          required: true
          schema:
            type: object
            properties:
              action:
                type: string
                description: ceph_config's action, it can be
                             config_reset/config_update
              ceph_config:
                type: object
                description: ceph_config object
                properties:
                  group:
                    type: string
                    description: ceph config file [group]
                  key:
                    type: string
                    description: cluster_configs key, it can be
                                 debug_osd/debug_mon/debug_rgw/osd_pool_default_type.
                  value:
                    type: string
                    description:  if the action is config_reset, you must give
                                  this value.
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        body = json_decode(self.request.body)
        validate(body, schema=update_ceph_config_schema,
                 format_checker=draft7_format_checker)
        action = body.get('action')
        if not action:
            raise exception.InvalidInput(reason=_("Ceph config: action is "
                                                  "none"))
        values = body.get('ceph_config')
        if not values:
            raise exception.InvalidInput(reason=_("Ceph config: config is "
                                                  "none!"))
        client = self.get_admin_client(ctxt)
        action_map = {
            "config_update": self._ceph_config_update,
        }
        fun_action = action_map.get(action)
        if fun_action is None:
            raise exception.DiskActionNotFound(action=action)
        config = yield fun_action(ctxt, client, values)

        self.write(objects.json_encode({
            "ceph_config": config
        }))


@URLRegistry.register(r"/ceph_configs/content/")
class CephConfigContentHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - ceph_config
        summary: Download the ceph_config file
        description: Return the ceph_config file and you can download it.
        operationId: ceph_configs.api.getCeph_configFile
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        content = yield client.ceph_config_content(ctxt)
        self.set_header('Content-Type', 'application/force-download')
        self.set_header('Content-Disposition',
                        'attachment; filename="ceph.conf"')
        self.write(content)
