#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json

from stor import objects
from stor.api.handlers.base import BaseAPIHandler


logger = logging.getLogger(__name__)


class VolumeHandler(BaseAPIHandler):
    def get(self):
        ctxt = self.get_context()
        volumes = objects.VolumeList.get_all(ctxt)
        self.write(json.dumps([
            {"id": vol.id} for vol in volumes
        ]))
