#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import traceback

from tornado.web import RequestHandler

from DSpace import exception
from DSpace.context import RequestContext
from DSpace.DSI.session import Session
from DSpace.DSM.client import AdminClientManager
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class BaseAPIHandler(RequestHandler):

    def initialize(self):
        self.session = Session(self)

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
        origin = self.request.headers.get('Origin')
        if not origin:
            return
        self.set_header("Access-Control-Allow-Origin", origin)
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Headers",
                        "x-requested-with, Content-Type, Cluster-Id, "
                        "x-access-module")
        self.set_header('Access-Control-Allow-Methods',
                        'POST, GET, OPTIONS, PUT, DELETE')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def bad_request(self, msg):
        self.set_status(400)
        self.write({"error": msg})

    def get_context(self):
        if not self.current_user:
            raise exception.NotAuthorized()
        ctxt = RequestContext(user_id=self.current_user.id, is_admin=False)
        client_ip = (self.request.headers.get("X-Real-IP") or
                     self.request.headers.get("X-Forwarded-For") or
                     self.request.remote_ip)
        ctxt.client_ip = client_ip
        return ctxt

    def get_paginated_args(self):
        return {
            "marker": self.get_query_argument('marker', default=None),
            "limit": self.get_query_argument('limit', default=None),
            "sort_keys": self.get_query_argument('sort_keys', default=None),
            "sort_dirs": self.get_query_argument('sort_dirs', default=None),
            "offset": self.get_query_argument('offset', default=None)
        }

    def get_metrics_history_args(self):
        start = self.get_query_argument('start', default=None)
        end = self.get_query_argument('end', default=None)
        if start and end:
            return {
                'start': start,
                'end': end,
            }
        else:
            raise exception.InvalidInput(
                reason=_("get_metrics_history_args: start and end required"))

    def get_current_user(self):
        logger.debug("User: %s", self.session['user'])
        return self.session['user']

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
        logger.exception(e)
        if isinstance(e, exception.StorException):
            self.send_error(e.code, reason=str(e))
        else:
            super(BaseAPIHandler, self)._handle_request_exception(e)

    def get_admin_client(self, ctxt):
        client = AdminClientManager(
            ctxt,
            async_support=True
        ).get_client()
        return client


class ClusterAPIHandler(BaseAPIHandler):

    def get_context(self):
        ctxt = super(ClusterAPIHandler, self).get_context()
        cluster_id = self.get_query_argument('cluster_id', default=None)
        if not cluster_id:
            cluster_id = self.request.headers.get('Cluster-Id')
        if not cluster_id:
            raise exception.ClusterIDNotFound()
        ctxt.cluster_id = cluster_id
        return ctxt
