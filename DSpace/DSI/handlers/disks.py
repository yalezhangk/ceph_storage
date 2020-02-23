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
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)


update_disk_type_schema = {
    "type": "object",
    "properties": {
        "disk": {
            "type": "object",
            "properties": {"type": {
                "type": "string",
                "enum": ["ssd", "hdd"]
            }}, "required": ["type"],
        },
    },
    "required": ["disk"],
}

update_disk_schema = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["light", "partition_create", "partition_remove",
                     "disk_replace_prepare", "disk_replace"]
        },
    },
    "allOf": [
        {
            "if": {
                "properties": {"action": {"const": "light"}}
            },
            "then": {
                "properties": {"disk": {
                    "type": "object",
                    "properties": {"led": {"enum": ["on", "off"]}},
                    "required": ["led"],
                    "additionalProperties": False
                }}
            }
        }, {
            "if": {
                "properties": {"action": {"const": "disk_replace"}}
            },
            "then": {
                "properties": {"disk": {
                    "type": "object",
                    "properties": {"new_disk_id": {"type": "number"}},
                    "required": ["new_disk_id"],
                    "additionalProperties": False
                }}
            }
        }, {
            "if": {
                "properties": {"action": {"const": "partition_create"}}
            },
            "then": {
                "properties": {"disk": {
                    "type": "object",
                    "properties": {
                        "partition_num": {"type": "number", "minimum": 1},
                        "role": {"enum": ["system", "data", "accelerate"]},
                        "partition_role": {
                            "enum": ["cache", "db", "wal", "journal", "mix"]
                        }},
                    "required": ["partition_num", "role", "partition_role"],
                    "additionalProperties": False
                }}
            }
        }, {
            "if": {
                "properties": {"action": {"const": "partition_remove"}}
            },
            "then": {
                "properties": {"disk": {
                    "type": "object",
                    "properties": {
                        "role": {"enum": ["system", "data", "accelerate"]},
                    },
                    "required": ["role"],
                    "additionalProperties": False
                }}
            }
        },
    ], "required": ["action", "disk"]
}


@URLRegistry.register(r"/disks/")
class DiskListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - disk
        summary: disk List
        description: Return a list of disks
        operationId: disks.api.listDisk
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
          name: tab
          description: Different tab page, it can be default or io
          type: string
          required: false
        - in: request
          name: node_id
          description: Node ID
          type: string
          required: false
        - in: request
          name: role
          description: disk role, it can be system/data/accelerate
          type: string
          required: false
        - in: request
          name: status
          description: disk status, it can be available/inuse
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        exact_filters = ['status', 'type', 'role', 'node_id']
        fuzzy_filters = ['name']
        filters = self.get_support_filters(exact_filters, fuzzy_filters)

        tab = self.get_query_argument('tab', default="default")
        if tab not in ["default", "io"]:
            raise exception.InvalidInput(_("this tab not support"))

        client = self.get_admin_client(ctxt)
        disks = yield client.disk_get_all(ctxt, tab=tab, filters=filters,
                                          **page_args)
        disk_count = yield client.disk_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "disks": disks,
            "total": disk_count
        }))


@URLRegistry.register(r"/disks/([0-9]*)/")
class DiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: Detail of the disk
        description: Return detail infomation of disk by id
        operationId: disks.api.diskDetail
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
          description: Disk ID
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
        disk = yield client.disk_get(ctxt, disk_id)
        self.write(objects.json_encode({
            "disk": disk
        }))

    @gen.coroutine
    def put(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: Update disk
        description: update disk.
        operationId: disks.api.updateDisk
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
          description: Disk ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: disk
          description: updated disk object
          required: true
          schema:
            type: object
            properties:
              disk:
                type: object
                properties:
                  type:
                    type: string
                    description: disk's type, it must be hdd/ssd.
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        body = json_decode(self.request.body)
        validate(body, schema=update_disk_type_schema,
                 format_checker=draft7_format_checker)
        disk = body.get('disk')
        disk_type = disk.get('type')
        if not disk_type:
            raise exception.InvalidInput(reason=_("disk: disk type is none"))

        client = self.get_admin_client(ctxt)
        disk = yield client.disk_update(ctxt, disk_id, disk_type)
        logger.debug("Disk: id: {}, name: {}, cluster_id: {}".format(
            disk.id, disk.name, disk.cluster_id
        ))

        self.write(objects.json_encode({
            "disk": disk
        }))


@URLRegistry.register(r"/disks/([0-9]*)/action/")
class DiskActionHandler(ClusterAPIHandler):
    def _disk_light(self, ctxt, client, disk_id, values):
        return client.disk_light(ctxt, disk_id=disk_id, led=values['led'])

    def _disk_partitions_create(self, ctxt, client, disk_id, values):
        required_args = ['partition_num', 'role', 'partition_role']
        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("disk: missing required arguments!"))
        if values['partition_num'] <= 0:
            raise exception.InvalidInput(
                reason=_("disk: partition_num must be greater than zero!"))
        return client.disk_partitions_create(ctxt, disk_id=disk_id,
                                             values=values)

    def _disk_partitions_remove(self, ctxt, client, disk_id, values):
        required_args = ['role']
        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("disk: missing required arguments!"))
        return client.disk_partitions_remove(ctxt, disk_id=disk_id,
                                             values=values)

    def _disk_replace_prepare(self, ctxt, client, disk_id, values):
        return client.osd_accelerate_disk_replace_prepare(ctxt,
                                                          disk_id=disk_id)

    def _disk_replace(self, ctxt, client, disk_id, values):
        return client.osd_accelerate_disk_replace(
            ctxt, disk_id=disk_id, new_disk_id=values.get("new_disk_id"))

    @gen.coroutine
    def post(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: Action of disk
        description: update disk. By this api, you can ligt the disk led,
                     create partition or remove partition.
                     But you can only do one thing at a time.
        operationId: disks.api.diskAction
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
          description: Disk ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: disk(light disk)
          description: open the disk's light
          required: true
          schema:
            type: object
            properties:
              action:
                type: string
                description: it must be light
              disk:
                type: object
                properties:
                  led:
                    type: string
                    description: disk led's status, it must be on/off.
        - in: body
          name: disk(partition create)
          description: create the disk's partition
          required: true
          schema:
            type: object
            properties:
              action:
                type: string
                description: it must be partition_create
              disk:
                type: object
                properties:
                  partition_num:
                    type: integer
                    format: int32
                    description: how much you partition want to create.
                  role:
                    type: string
                    description: disk role, it must be system/data/accelerate.
                  partition_role:
                    type: string
                    description: disk partition role,
                                 it must be cache/db/wal/journal/mix.
        - in: body
          name: disk(partition remove)
          description: remove the disk's partition
          required: true
          schema:
            type: object
            properties:
              action:
                type: string
                description: it must be partition_remove
              disk:
                type: object
                properties:
                  role:
                    type: string
                    description: disk role, it must be system/data/accelerate.
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        body = json_decode(self.request.body)
        validate(body, schema=update_disk_schema,
                 format_checker=draft7_format_checker)
        action = body.get('action')
        if not action:
            raise exception.InvalidInput(reason=_("disk: action is none"))
        disk = body.get('disk')

        client = self.get_admin_client(ctxt)
        action_map = {
            "light": self._disk_light,
            "partition_create": self._disk_partitions_create,
            "partition_remove": self._disk_partitions_remove,
            "disk_replace_prepare": self._disk_replace_prepare,
            "disk_replace": self._disk_replace,
        }
        fun_action = action_map.get(action)
        if fun_action is None:
            raise exception.DiskActionNotFound(action=action)
        disk = yield fun_action(ctxt, client, disk_id, disk)

        self.write(objects.json_encode({
            'disk': disk
        }))


@URLRegistry.register(r"/disks/([0-9]*)/smart/")
class DiskSmartHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: smart infomation of the disk
        description: Return smart infomation of disk by id
        operationId: disks.api.diskSmartInfo
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
          description: Disk ID
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
        smart = yield client.disk_smart_get(ctxt, disk_id)
        self.write(objects.json_encode({
            "disk_smart": smart
        }))


@URLRegistry.register(r"/disks/([0-9]*)/perf/")
class DiskPerfHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: Disk's Performance
        description: return the performance of disk by id
        operationId: disks.api.getPerformance
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
          description: disk's id
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
        data = yield client.disk_perf_get(ctxt, disk_id)
        self.write(json.dumps({
            "disk_perf": data
        }))


@URLRegistry.register(r"/disks/([0-9]*)/history_perf/")
class DiskPerfHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        """
        ---
        tags:
        - disk
        summary: disk's History Performance
        description: return the History Performance of disk by id
        operationId: disks.api.getHistoryPerformance
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
          description: disk's id
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
        data = yield client.disk_perf_history_get(
            ctxt, disk_id=disk_id, start=his_args['start'],
            end=his_args['end'])
        self.write(json.dumps({
            "disk_history_perf": data
        }))


@URLRegistry.register(r"/disk_available/")
class DiskAvailableListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - disk
        summary: available disk List
        description: Return a list of abailable disks
        operationId: disks.api.listAvailableDisk
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
          description: Node ID
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()

        filters = {}
        supported_filters = ['node_id']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        client = self.get_admin_client(ctxt)
        expected_attrs = ['node']
        disks = yield client.disk_get_all_available(
            ctxt, filters=filters, expected_attrs=expected_attrs)
        self.write(objects.json_encode({
            "disks": disks,
        }))


@URLRegistry.register(r"/disks/top/")
class DiskTopHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - disk
        summary: disk io top
        description: Return a list of io top disks
        operationId: disks.api.listIoTopDisk
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
          name: k
          description: number of disk
          type: integer
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        k = self.get_query_argument('k', None)
        if not k:
            k = 10
        elif k.isdigit():
            k = int(k)
        else:
            raise exception.InvalidInput(_("k must be number"))
        client = self.get_admin_client(ctxt)
        disks = yield client.disk_io_top(ctxt, k)
        self.write(objects.json_encode({
            "disks": disks,
        }))
