#!/usr/bin/env python
# -*- coding: utf-8 -*-
from t2stor.api.handlers.volumes import VolumeHandler
from t2stor.api.handlers.volumes import VolumeListHandler
from t2stor.api.handlers.clusters import ClusterHandler
from t2stor.api.handlers.clusters import ClusterDetectHandler
from t2stor.api.handlers.rpc_service import RpcServiceListHandler
from t2stor.api.handlers.licenses import LicenseHandler


def get_routers():
    return [
        (r"/clusters", ClusterHandler),
        (r"/cluster_detect", ClusterDetectHandler),
        (r"/rpc_services", RpcServiceListHandler),
        (r"/volumes", VolumeListHandler),
        (r"/volumes/([0-9]*)", VolumeHandler),
        (r"/licenses/", LicenseHandler),
    ]
