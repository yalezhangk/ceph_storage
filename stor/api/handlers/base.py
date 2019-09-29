#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import traceback

from tornado.web import RequestHandler

from stor.context import RequestContext
from stor import exception
from stor.admin.client import AdminClientManager


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
        return RequestContext(user_id="xxx", project_id="stor", is_admin=False)

    def get_current_user(self):
        token_id = self.request.headers.get('token-id')
        if not token_id:
            return None
        user = None
        if not user:
            return None
        return user

    def write_error(self, status_code, **kwargs):
        """Override to implement custom error pages.

        ``write_error`` may call `write`, `render`, `set_header`, etc
        to produce output as usual.

        If this error was caused by an uncaught exception (including
        HTTPError), an ``exc_info`` triple will be available as
        ``kwargs["exc_info"]``.  Note that this exception may not be
        the "current" exception for purposes of methods like
        ``sys.exc_info()`` or ``traceback.format_exc``.
        """
        if self.settings.get("serve_traceback") and "exc_info" in kwargs:
            # in debug mode, try to send a traceback
            self.set_header("Content-Type", "text/plain")
            for line in traceback.format_exception(*kwargs["exc_info"]):
                self.write(line)
            self.finish()
        else:
            self.set_header("Content-Type", "application/json")
            self.finish(json.dumps({
                "code": status_code,
                "message": self._reason
            }))

    def _handle_request_exception(self, e):
        """Overide to handle StorException"""
        if isinstance(e, exception.StorException):
            self.send_error(e.code, reason=str(e))
        else:
            super(BaseAPIHandler, self)._handle_request_exception(e)


class RPCAPIHandler(BaseAPIHandler):

    def get_context(self):
        cluster_id = self.get_query_argument('cluster_id', default=None)
        if not cluster_id:
            cluster_id = self.request.headers.get('Cluster-Id')
        if not cluster_id:
            raise exception.ClusterIDNotFound()
        return RequestContext(user_id="xxx", project_id="stor", is_admin=False,
                              cluster_id=cluster_id)

    def get_manager_client(self, ctxt):
        client = AdminClientManager(
            ctxt,
            cluster_id=ctxt.cluster_id[0:8],
            async_support=True
        ).get_client()
        return client
