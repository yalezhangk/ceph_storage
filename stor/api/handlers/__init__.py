#!/usr/bin/env python
# -*- coding: utf-8 -*-
from stor.api.handlers.volumes import VolumeHandler
from stor.api.handlers.clusters import ClusterHandler


def get_routers():
    return [
        (r"/volumes", VolumeHandler),
        (r"/clusters", ClusterHandler),
    ]
