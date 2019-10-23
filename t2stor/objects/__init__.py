#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from t2stor.objects.base import JsonEncoder


def register_all():
    # NOTE(danms): You must make sure your object gets imported in this
    # function in order for it to be registered by services that may
    # need to receive it via RPC.
    __import__('t2stor.objects.volume')
    __import__('t2stor.objects.cluster')
    __import__('t2stor.objects.rpc_service')
    __import__('t2stor.objects.node')
    __import__('t2stor.objects.datacenter')
    __import__('t2stor.objects.rack')
    __import__('t2stor.objects.osd')
    __import__('t2stor.objects.sysconfig')
    __import__('t2stor.objects.volume_access_path')
    __import__('t2stor.objects.volume_gateway')
    __import__('t2stor.objects.volume_client')
    __import__('t2stor.objects.volume_client_group')
    __import__('t2stor.objects.pool')
    __import__('t2stor.objects.license')
    __import__('t2stor.objects.alert_rule')


def json_encode(obj):
    return json.dumps(obj, cls=JsonEncoder)
