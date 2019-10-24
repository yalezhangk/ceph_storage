#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class LogFileListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        log_files = yield client.log_file_get_all(ctxt)
        self.write(objects.json_encode({
            "log_files": log_files
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
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        log_file = yield client.log_file_get(ctxt, log_file_id)
        self.write(objects.json_encode({
            "log_file": log_file
        }))

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
