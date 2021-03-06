#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


CHUNK_SIZE = 1048576


@URLRegistry.register(r"/log_files/")
class LogFileListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - log_file
        summary: log file List
        description: Return a list of log files
        operationId: logfiles.api.listLogFile
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
          name: node_id
          description: Node ID
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: service_type
          description: type of log file service, it can be mon/osd
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        # 日志文件列表(元数据)
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        node_id = self.get_argument('node_id')
        page_args = self.get_paginated_args()
        service_type = self.get_argument('service_type')
        log_files = yield client.log_file_get_all(
            ctxt, node_id, service_type, **page_args)
        log_file_count = yield client.log_file_get_count(
            ctxt, node_id, service_type)
        self.write(objects.json_encode({
            "log_files": log_files,
            "total": log_file_count
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("log_file")
        client = self.get_admin_client(ctxt)
        log_file = yield client.log_file_create(ctxt, data)
        self.write(objects.json_encode({
            "log_file": log_file
        }))


@URLRegistry.register(r"/log_files/([0-9]*)/")
class LogFileHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, log_file_id):
        """
        ---
        tags:
        - log_file
        summary: download the log file by id
        description: download log file by id
        operationId: logfiles.api.downloadLogFile
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
          description: Log file's ID
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        # 日志文件下载
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        file_size = yield client.log_file_size(ctxt, log_file_id)
        file_seg_size = CHUNK_SIZE
        file_segs = int(file_size / file_seg_size) + 1
        for i in range(0, file_segs):
            logger.info("get file content, offset: %s", i * file_seg_size)
            file_message = yield client.download_log_file(
                ctxt, log_file_id, i * file_seg_size, file_seg_size)
            file_name = file_message['file_name']
            file_content = file_message['content']

            self.set_header('Content-Type', 'application/octet-stream')
            self.set_header('Content-Disposition',
                            'attachment; filename={}'.format(file_name))
            file_content = base64.b64decode(file_content)
            self.write(file_content)
            self.flush()

    @gen.coroutine
    def put(self, log_file_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        log_file_data = data.get('log_file')
        client = self.get_admin_client(ctxt)
        log_file = yield client.log_file_update(ctxt, log_file_id,
                                                log_file_data)
        self.write(objects.json_encode({
            "log_file": log_file
        }))

    @gen.coroutine
    def delete(self, log_file_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        log_file = yield client.log_file_delete(ctxt, log_file_id)
        self.write(objects.json_encode({
            "log_file": log_file
        }))
