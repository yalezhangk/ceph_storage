#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/tasks/")
class TaskListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """Task list
        ---
        tags:
        - task
        summary: Task List
        description: Return a list of task
        operationId: tasks.api.list
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
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        client = self.get_admin_client(ctxt)
        tasks = yield client.task_get_all(ctxt, **page_args)
        task_count = yield client.task_get_count(ctxt)
        self.write(objects.json_encode({
            "tasks": tasks,
            "total": task_count
        }))
