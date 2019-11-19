#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.utils import cluster_config

logger = logging.getLogger(__name__)


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
        ceph_config = yield client.ceph_config_get_all(ctxt,
                                                       filters=filters,
                                                       **page_args)
        ceph_config_count = yield client.ceph_config_get_count(
            ctxt, filters=filters)
        self.write(objects.json_encode({
            "ceph_configs": ceph_config,
            "total": ceph_config_count
        }))


@URLRegistry.register(r"/ceph_configs/action/")
class CephConfigActionHandler(ClusterAPIHandler):
    def _ceph_config_set(self, ctxt, client, action, values):
        if action == 'update':
            required_args = ['group', 'key', 'value', 'value_type']
        if action == 'reset':
            required_args = ['group', 'key']

        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("Ceph config: missing required arguments!"))

        detail = cluster_config.cluster_configs.get(values['key'])
        if not detail:
            raise exception.InvalidInput(
                reason=_("{} do not support to modify".format(values['key']))
            )
        if action == 'update':
            if values['value_type'] != detail.get('type'):
                raise exception.InvalidInput(
                    reason=_("Type of config is error, it needs to be".format(
                        detail.get('type')))
                )
        if action == 'reset':
            values.update({'value': detail.get('default')})

        return client.ceph_config_set(ctxt, values=values)

    def _ceph_config_update(self, ctxt, client, values):
        return self._ceph_config_set(ctxt, client, 'update', values)

    def _ceph_config_reset(self, ctxt, client, values):
        return self._ceph_config_set(ctxt, client, 'reset', values)

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
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        body = json_decode(self.request.body)
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
            "config_reset": self._ceph_config_reset,
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
