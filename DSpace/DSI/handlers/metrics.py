#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen

from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import AnonymousHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/metrics")
class MetricsHandler(AnonymousHandler):
    @gen.coroutine
    def get(self):
        """Metrics
        ---
        tags:
        - task
        summary: Metrics Info
        description: Return a metrics
        operationId: metrics.api.list
        produces:
        - text/plain
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        content = yield client.metrics_content(ctxt)
        self.set_header("Content-Type", "text/plain")
        self.write(content)
