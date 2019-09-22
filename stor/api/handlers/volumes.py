#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json

from stor.api.handlers.base import RPCAPIHandler
from stor.manager.client import ManagerClientManager


logger = logging.getLogger(__name__)


class VolumeListHandler(RPCAPIHandler):
    def get(self):
        ctxt = self.get_context()
        client = ManagerClientManager(
            cluster_id='7be530ce'
        ).get_client("devel")
        volumes = client.volume_get_all(ctxt)
        self.write(json.dumps([
            {"id": vol.id, "name": vol.name} for vol in volumes
        ]))


class VolumeHandler(RPCAPIHandler):
    def get(self, volume_id):
        ctxt = self.get_context()
        client = ManagerClientManager(
            cluster_id='7be530ce'
        ).get_client("devel")
        volume = client.volume_get(ctxt, volume_id)
        self.write(json.dumps({"id": volume.id, "name": volume.name}))
