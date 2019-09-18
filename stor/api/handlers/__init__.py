#!/usr/bin/env python
# -*- coding: utf-8 -*-
from stor.api.handlers.volumes import VolumeHandler


def get_routers():
    return [
        (r"/volumes", VolumeHandler),
    ]
