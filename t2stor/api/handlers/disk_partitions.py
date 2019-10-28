#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

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

        client = self.get_admin_client(ctxt)
        disk_parts = yield client.disk_partition_get_all(
            ctxt, filters=filters, **page_args)

        self.write(objects.json_encode({
            "disk_partitions": disk_parts
        }))
