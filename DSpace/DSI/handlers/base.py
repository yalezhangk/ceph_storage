#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import traceback

from jsonschema.exceptions import ValidationError
from tornado.web import RequestHandler

from DSpace import context
from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.context import get_context
from DSpace.DSI.auth import AuthRegistry
from DSpace.DSI.session import get_session
from DSpace.DSM.client import AdminClientManager
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class AnonymousHandler(RequestHandler):

    def get_context(self):
        return get_context()

    def bad_request(self, msg):
        self.set_status(400)
        self.write({"error": msg})

    def get_admin_client(self, ctxt):
        if not hasattr(context, 'dsm_client'):
            dsm_client = AdminClientManager(
                ctxt,
                async_support=True
            ).get_client()
            setattr(context, 'dsm_client', dsm_client)
        return getattr(context, 'dsm_client')

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
            # log exception
            if e.code >= 500:
                logger.exception("%s raise exception: %s", self.request.uri, e)
        elif isinstance(e, ValidationError):
            # tornado will log exception
            message = '.'.join([str(i) for i in e.path])
            message += ": " + e.message
            self.send_error(400, reason=message)
        else:
            # log exception
            logger.exception("%s raise exception: %s", self.request.uri, e)
            super(AnonymousHandler, self)._handle_request_exception(e)

    def api_log_exception(self, op, e):
        if isinstance(e, exception.StorException):
            if e.code < 500:
                # exception content will auto add in end
                logger.warning("url(%s), op(%s)", self.request.uri, op)
                return
        logger.exception("%s raise exception: %s", self.request.uri, e)


class BaseAPIHandler(AnonymousHandler):
    ctxt = None
    auth = None

    def initialize(self):
        session_cls = get_session()
        self.session = session_cls(self)
        self._setup_auth()

    def _setup_auth(self):
        if not self.auth:
            registry = AuthRegistry()
            self.auth = registry.auth_cls()

    def prepare(self):
        if self.request.method != "OPTIONS":
            self.ctxt = self.get_context()
        logger.info("uri(%s), method(%s), body(%s)",
                    self.request.uri, self.request.method, self.request.body)

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
        origin = self.request.headers.get('Origin')
        if not origin:
            return
        self.set_header("Access-Control-Allow-Origin", origin)
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Headers",
                        "x-requested-with, Content-Type, X-Cluster-Id, "
                        "x-access-module")
        self.set_header('Access-Control-Allow-Methods',
                        'POST, GET, OPTIONS, PUT, DELETE')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def get_context(self):
        if self.ctxt:
            return self.ctxt
        ctxt = RequestContext(user_id="anonymous", is_admin=False,
                              ws_ip=CONF.my_ip)
        self.auth.validate(ctxt, self)
        if self.current_user:
            user_id = self.current_user.id
        else:
            # no current_user
            user_id = None
        ctxt.user_id = user_id
        logger.debug("Context: %s", ctxt.to_dict())
        client_ip = (self.request.headers.get("X-Real-IP") or
                     self.request.headers.get("X-Forwarded-For") or
                     self.request.remote_ip)
        ctxt.client_ip = client_ip
        cluster_id = self.get_cluster_id()
        if cluster_id:
            objects.Cluster.get_by_id(ctxt, cluster_id)
        ctxt.cluster_id = cluster_id
        self.ctxt = ctxt
        return self.ctxt

    def get_paginated_args(self):
        sort_key = self.get_query_argument('sort_key', default=None)
        sort_dir = self.get_query_argument('sort_dir', default=None)
        limit = self.get_query_argument('limit', default=None) or None
        offset = self.get_query_argument('offset', default=None) or None
        marker = self.get_query_argument('marker', default=None) or None
        return {
            "marker": marker,
            "limit": limit,
            "sort_keys": [sort_key] if sort_key else None,
            "sort_dirs": [sort_dir] if sort_dir else None,
            "offset": offset
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

    def get_cluster_id(self):
        cluster_id = self.get_query_argument('cluster_id', default=None)
        if not cluster_id:
            cluster_id = self.request.headers.get('X-Cluster-Id')
        return cluster_id

    def get_support_filters(self, exact_filters=None, fuzzy_filters=None):
        exact_filters = exact_filters or []
        fuzzy_filters = fuzzy_filters or []
        filters = {}
        for e in exact_filters:
            # 精确字段
            value = self.get_query_argument(e, default=None)
            if value:
                filters.update({e: value})
        for f in fuzzy_filters:
            # 模糊字段
            value = self.get_query_argument(f, default=None)
            if value:
                fuzzy_filter = '{}~'.format(f)
                filters.update({fuzzy_filter: value})
        return filters

    def redirect_url(self, url):
        logger.debug('redirect_url is:%s' % url)
        self.set_status(401)
        self.finish(objects.json_encode({
            "type": "External",
            "url": url
        }))
        self.flush()


class ClusterAPIHandler(BaseAPIHandler):

    def get_context(self):
        ctxt = super(ClusterAPIHandler, self).get_context()
        if not ctxt.cluster_id:
            raise exception.ClusterIDNotFound()
        return ctxt
