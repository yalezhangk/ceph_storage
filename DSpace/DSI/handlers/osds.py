#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.i18n import _
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)

create_osd_schema = {
    "definitions": {
        "osd": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["bluestore", "filestore"]},
                "disk_id": {"type": "integer"},
                "cache_partition_id": {"type": ["integer", "null"]},
                "db_partition_id": {"type": ["integer", "null"]},
                "wal_partition_id": {"type": ["integer", "null"]},
                "journal_partition_id": {"type": ["integer", "null"]},
            },
            "required": ["type", "disk_id"],
        }
    },
    "type": "object",
    "properties": {
        "osd": {"$ref": "#/definitions/osd"},
        "osds": {
            "type": "array",
            "items": {"type": "object", "$ref": "#/definitions/osd"},
            "minItems": 1
        },
    },
    "anyOf": [{"required": ["osd"]}, {"required": ["osds"]}]
}

update_osd_schema = {
    "type": "object",
    "properties": {
        "new_disk_id": {"type": "integer"}
    },
    "required": ["new_disk_id"],
}


@URLRegistry.register(r"/osds/")
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

        exact_filters = ['status', 'type', 'disk_type', 'node_id', 'disk_id']
        filters = self.get_support_filters(exact_filters, [])
        osd_id = self.get_query_argument('osd_id', default=None)
        if osd_id:
            filters = self._osd_filters(osd_id, filters)
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

    def _osd_filters(self, osd_id, filters):
        osd_name = osd_id.strip()
        if osd_name in 'osd.':
            # ??????
            pass
        else:
            osd_li = osd_name.split('osd.')
            if len(osd_li) == 2:
                # ??????^ osd.0
                osd_id = osd_li[1]
                filters.update({'^osd_id': osd_id})
            else:
                # ?????? 0
                osd_id = osd_li[0]
                filters.update({'osd_id~': osd_id})
        return filters

    def _osd_create_check(self, osd):
        osd_type = osd.get("type")
        journal_id = osd.get('journal_partition_id')
        if not journal_id:
            osd.pop('journal_partition_id', None)
        db_id = osd.get('db_partition_id')
        if not db_id:
            osd.pop('db_partition_id', None)
        wal_id = osd.get('wal_partition_id')
        if not wal_id:
            osd.pop('wal_partition_id', None)
        cache_id = osd.get('cache_partition_id')
        if not cache_id:
            osd.pop('cache_partition_id', None)
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
                osd:
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
            type: object
            properties:
                osds:
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
        validate(data, schema=create_osd_schema,
                 format_checker=draft7_format_checker)
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
            tasks = []
            osds = []
            for data in datas:
                self._osd_create_check(data)
            for data in datas:
                task = client.osd_create(ctxt, data)
                tasks.append(task)
            for task in tasks:
                try:
                    osd = yield task
                    osds.append(osd)
                except exception.ClusterNotHealth as e:
                    self.api_log_exception("create osd", e)
                    err_ms = str(e)
                    wb_client = WebSocketClientManager(
                        context=ctxt).get_client(CONF.my_ip)
                    wb_client.send_message(
                        ctxt, data, "CREATE_ERROR", err_ms,
                        resource_type='Osd')
                except Exception as e:
                    self.api_log_exception("create osd", e)
                    err_ms = str(e)
                    wb_client = WebSocketClientManager(
                        context=ctxt).get_client(CONF.my_ip)
                    wb_client.send_message(
                        ctxt, data, "CREATE_ERROR", err_ms,
                        resource_type='Osd')

            self.write(objects.json_encode({
                "osds": osds
            }))
        else:
            raise ValueError("data not accept: %s", data)


@URLRegistry.register(r"/osds/([0-9]*)/")
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
                          'pools', 'cache_partition', 'journal_partition']
        osd = yield client.osd_get(
            ctxt, osd_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({
            "osd": osd
        }))

    @gen.coroutine
    def delete(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Delete the osd by id
        description: delete osd by id
        operationId: osds.api.deleteOsd
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
          description: Osd's id
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
        osd = yield client.osd_delete(ctxt, osd_id)
        self.write(objects.json_encode({
            "osd": osd
        }))


@URLRegistry.register(r"/osds/([0-9]*)/disk_replace_prepare/")
class OsdDiskReplacePrepareHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: disk replace prepare
        description: clean osd for osd replace
        operationId: osds.api.putDiskReplacePrepare
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
        osd = yield client.osd_disk_replace_prepare(ctxt, osd_id)
        self.write(objects.json_encode({
            "osd": osd
        }))


@URLRegistry.register(r"/osds/([0-9]*)/disk_replace/")
class OsdDiskReplaceHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: disk replace
        description: recreate osd on new disk
        operationId: osds.api.putDiskReplace
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
        data = json_decode(self.request.body)
        validate(data, schema=update_osd_schema,
                 format_checker=draft7_format_checker)
        new_disk_id = data.get("new_disk_id")
        client = self.get_admin_client(ctxt)
        osd = yield client.osd_disk_replace(ctxt, osd_id, new_disk_id)
        self.write(objects.json_encode({
            "osd": osd
        }))


@URLRegistry.register(r"/osds/([0-9]*)/metrics/")
class OsdMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Osd's Metrics
        description: return the Metrics of osd by id
        operationId: osds.api.getMetrics
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
          description: Osd's id
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
        data = yield client.osd_metrics_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_metrics": data
        }))


@URLRegistry.register(r"/osds/([0-9]*)/history_metrics/")
class OsdMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Osd's History Metrics
        description: return the History Metrics of osd by id
        operationId: pools.api.getHistoryMetrics
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
          description: Osd's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_metrics_history_get(
            ctxt, osd_id=osd_id, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "osd_metrics_history": data
        }))


@URLRegistry.register(r"/osds/([0-9]*)/disk_metrics/")
class OsdDiskMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Osd's Disk Metrics
        description: return the Metrics of osd's disk by id
        operationId: osds.api.getDiskMetrics
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
          description: Osd's id
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
        data = yield client.osd_disk_metrics_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_disk_metrics": data
        }))


@URLRegistry.register(r"/osds/([0-9]*)/history_disk_metrics/")
class OsdHistoryDiskMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        """
        ---
        tags:
        - osd
        summary: Osd's disk History Metrics
        description: return the History Metrics of osd's disk by id
        operationId: pools.api.getDiskHistoryMetrics
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
          description: Osd's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_history_disk_metrics_get(
            ctxt, osd_id=osd_id, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "osd_history_disk_metrics": data
        }))


@URLRegistry.register(r"/osds/([0-9]*)/capacity/")
class OsdCapacityHandler(ClusterAPIHandler):
    """
    ---
    tags:
    - osd
    summary: Osd's Capacity
    description: return the Capacity of osd by id
    operationId: osds.api.getOsdCapacity
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
      description: Osd's id
      schema:
        type: integer
        format: int32
      required: true
    responses:
    "200":
      description: successful operation
    """
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_capacity_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_capacity": data
        }))


@URLRegistry.register(r"/osds/available/")
class OsdAvailableHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """List osds fro pool

        ---
        tags:
        - osd
        summary: List osds for pool
        description: Return a list of osds.
        operationId: osds.api.listOsdForPool
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
        expected_attrs = ['node', 'disk']

        exact_filters = ['type', 'disk_type']
        filters = self.get_support_filters(exact_filters)
        filters['status'] = s_fields.OsdStatus.ACTIVE
        filters['crush_rule_id'] = None

        osds = yield client.osd_get_all(
            ctxt, tab="default", filters=filters,
            expected_attrs=expected_attrs)

        self.write(objects.json_encode({
            "osds": osds,
        }))


@URLRegistry.register(r"/osds/slow_requests/")
class OsdSlowRequests(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osd_top = self.get_query_argument('osd_top', default=10)
        if osd_top and not osd_top.isdecimal():
            raise exception.InvalidInput(_("osd_top not number"))
        if osd_top:
            osd_top = int(osd_top)
        op_top = self.get_query_argument('op_top', default=10)
        if op_top and not op_top.isdecimal():
            raise exception.InvalidInput(_("op_top not number"))
        if op_top:
            op_top = int(op_top)
        res = yield client.osd_slow_requests_get(ctxt, osd_top, op_top)
        self.write(objects.json_encode({"slow_requests": res}))
