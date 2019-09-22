#!/usr/bin/env python
# -*- coding: utf-8 -*-
from tornado.web import RequestHandler

from stor.context import RequestContext
from stor import objects


class BaseAPIHandler(RequestHandler):
    user = None

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers",
                        "x-requested-with, Content-Type, token-id")
        self.set_header('Access-Control-Allow-Methods',
                        'POST, GET, OPTIONS, PUT, DELETE')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def bad_request(self, msg):
        self.set_status(400)
        self.write({"error": msg})

    def get_context(self):
        cluster_id = self.request.headers.get('Cluster-Id')
        if cluster_id:
            cluster = objects.Cluster(
                id=cluster_id, table_id="stor-%s" % cluster_id[0:8])
        else:
            cluster = None
        return RequestContext(user_id="xxx", project_id="stor", is_admin=False,
                              cluster=cluster)

    def get_current_user(self):
        token_id = self.request.headers.get('token-id')
        if not token_id:
            return None
        user = None
        if not user:
            return None
        return user
