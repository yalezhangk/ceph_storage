#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class OsdListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """List osds

        ---
        tags:
        - osd
        summary: List osds
        description: Return a list of osds.
        operationId: osds.api.listOsd
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
          name: node_id
          description: Filter the list of osds by node ID
          schema:
            type: integer
            format: int32
          required: false
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
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()
        client = self.get_admin_client(ctxt)
        expected_attrs = ['node', 'disk', 'pools', 'db_partition',
                          'wal_partition', 'cache_partition',
                          'journal_partition']

        filters = {}
        supported_filters = ['node_id']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        tab = self.get_query_argument('tab', default="default")
        if tab not in ["default", "io"]:
            raise exception.InvalidInput(_("this tab not support"))

        osds = yield client.osd_get_all(
            ctxt, tab=tab, filters=filters, expected_attrs=expected_attrs,
            **page_args)

        osd_count = yield client.osd_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "osds": osds,
            "total": osd_count
        }))

    def _osd_create_check(self, osd):
        osd_type = osd.get("type")
        journal_id = osd.get('journal_partition_id')
        db_id = osd.get('db_partition_id')
        wal_id = osd.get('wal_partition_id')
        cache_id = osd.get('cache_partition_id')
        if osd_type not in s_fields.OsdType.ALL:
            raise exception.InvalidInput(_("Osd type err"))
        if osd_type == s_fields.OsdType.BLUESTORE:
            if journal_id:
                raise exception.InvalidInput(
                    _("BlueStore not support journal"))
        elif osd_type == s_fields.OsdType.FILESTORE:
            if db_id or wal_id or cache_id:
                raise exception.InvalidInput(
                    _("FileStore not support DB, WAL or Cache"))

    @gen.coroutine
    def post(self):
        """Create osd

        ---
        tags:
        - osd
        summary: Create osd
        description: Create osd or osds.
        operationId: osds.api.createOsd
        produces:
        - application/json
        parameters:
        - in: body
          name: osd
          description: Created osd object
          required: false
          schema:
            type: object
            properties:
              type:
                type: string
                description: bluestore or filestore
              disk_id:
                type: integer
                format: int32
              cache_partition_id:
                type: integer
                format: int32
              db_partition_id:
                type: integer
                format: int32
              wal_partition_id:
                type: integer
                format: int32
              jounal_partition_id:
                type: integer
                format: int32
        - in: body
          name: osds
          description: Created multiple osd object
          required: false
          schema:
            type: array
            items:
              type: object
              properties:
                type:
                  type: string
                  description: bluestore or filestore
                disk_id:
                  type: integer
                  format: int32
                cache_partition_id:
                  type: integer
                  format: int32
                db_partition_id:
                  type: integer
                  format: int32
                wal_partition_id:
                  type: integer
                  format: int32
                jounal_partition_id:
                  type: integer
                  format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        if 'osd' in data:
            data = data.get('osd')
            client = self.get_admin_client(ctxt)
            self._osd_create_check(data)
            osd = yield client.osd_create(ctxt, data)

            self.write(objects.json_encode({
                "osd": osd
            }))
        elif 'osds' in data:
            datas = data.get('osds')
            client = self.get_admin_client(ctxt)
            osds = []
            for data in datas:
                self._osd_create_check(data)
            for data in datas:
                try:
                    osd = yield client.osd_create(ctxt, data)
                    osds.append(osd)
                except Exception:
                    pass

            self.write(objects.json_encode({
                "osds": osds
            }))
        else:
            raise ValueError("data not accept: %s", data)


class OsdHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Detail of the osd
        description: Return detail infomation of osd by id
        operationId: osds.api.osdDetail
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
          description: Osd ID
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
        expected_attrs = ['node', 'disk', 'db_partition', 'wal_partition',
                          'cache_partition', 'journal_partition']
        osd = yield client.osd_get(
            ctxt, osd_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({
            "osd": osd
        }))

    @gen.coroutine
    def delete(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osd = yield client.osd_delete(ctxt, osd_id)
        self.write(objects.json_encode({
            "osd": osd
        }))


class OsdMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_metrics_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_metrics": data
        }))


class OsdMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_metrics_history_get(
            ctxt, osd_id=osd_id, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "osd_metrics_history": data
        }))


class OsdDiskMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_disk_metrics_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_disk_metrics": data
        }))


class OsdHistoryDiskMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_history_disk_metrics_get(
            ctxt, osd_id=osd_id, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "osd_history_disk_metrics": data
        }))
