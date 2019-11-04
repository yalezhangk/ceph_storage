#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class LogFileListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        # 日志文件列表(元数据)
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        node_id = self.get_argument('node_id')
        page_args = self.get_paginated_args()
        service_type = self.get_argument('service_type')
        log_files = yield client.log_file_get_all(ctxt, node_id, service_type,
                                                  **page_args)
        log_files_all = yield client.log_file_get_all(
            ctxt, node_id, service_type)
        self.write(objects.json_encode({
            "log_files": log_files,
            "total": len(log_files_all)
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


class LogFileHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, log_file_id):
        # 日志文件下载
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        file_path = yield client.log_file_get(ctxt, log_file_id)
        filename = file_path.split('/')[-1]
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition',
                        'attachment; filename={}'.format(filename))
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(512)
                if not data:
                    # del log file
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    break
                self.write(data)
        # todo stream 传输
        self.finish()

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
