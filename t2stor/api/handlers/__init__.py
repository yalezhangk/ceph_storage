#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler
from t2stor.api.handlers.clusters import ClusterHandler


def get_routers():
    return [
        (r"/clusters", ClusterHandler),
        (r"/volumes", VolumeListHandler),
        (r"/volumes/([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})", VolumeHandler),
    ]
