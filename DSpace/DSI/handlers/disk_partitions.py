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
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        supported_filters = ['disk_id']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        expected_attrs = ['disk', 'node']
        client = self.get_admin_client(ctxt)
        disk_parts = yield client.disk_partition_get_all(
            ctxt, filters=filters, expected_attrs=expected_attrs, **page_args)
        disk_parts_all = yield client.disk_partition_get_all(
            ctxt, filters=filters)

        self.write(objects.json_encode({
            "disk_partitions": disk_parts,
            "total": len(disk_parts_all)
        }))
