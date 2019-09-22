#!/usr/bin/env python
# -*- coding: utf-8 -*-
from stor.api.handlers.volumes import VolumeHandler
from stor.api.handlers.volumes import VolumeListHandler
from stor.api.handlers.clusters import ClusterHandler


def get_routers():
    return [
        (r"/clusters", ClusterHandler),
        (r"/volumes", VolumeListHandler),
        (r"/volumes/([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})", VolumeHandler),
    ]
