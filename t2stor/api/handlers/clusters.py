#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json

from t2stor import objects
from t2stor.api.handlers.base import BaseAPIHandler


logger = logging.getLogger(__name__)


class ClusterHandler(BaseAPIHandler):
    def get(self):
        ctxt = self.get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        self.write(json.dumps([
            {"id": vol.id} for vol in clusters
        ]))

    def post(self):
        ctxt = self.get_context()
        name = self.get_argument('name')
        cluster = objects.Cluster(ctxt, display_name=name)
        cluster.create()
        self.write(json.dumps(
            {"id": cluster.id, "name": cluster.name}
        ))
