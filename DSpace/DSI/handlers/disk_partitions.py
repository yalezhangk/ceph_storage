#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class DiskPartitionListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - disk_partition
        summary: disk_partition List
        description: Return a list of disk partitions
        operationId: disk_partitions.api.listDiskPartition
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
          name: disk_id
          description: get the disk's partition by disk id
          type: integer
          format: int32
          required: false
        - in: request
          name: role
          description: filter by the disk partition role,
                       it can be cache/db/wal/journal/mix
          type: string
          required: false
        - in: request
          name: status
          description: filter by the disk partition status,
                       it can be available/inuse
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        supported_filters = ['disk_id', 'role', 'status']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        expected_attrs = ['disk', 'node']
        client = self.get_admin_client(ctxt)
        disk_partitions = yield client.disk_partition_get_all(
            ctxt, filters=filters, expected_attrs=expected_attrs, **page_args)
        disk_partition_count = yield client.disk_partition_get_count(
            ctxt, filters=filters)
        self.write(objects.json_encode({
            "disk_partitions": disk_partitions,
            "total": disk_partition_count
        }))
