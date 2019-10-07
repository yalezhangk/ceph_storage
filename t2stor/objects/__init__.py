#!/usr/bin/env python
# -*- coding: utf-8 -*-


def register_all():
    # NOTE(danms): You must make sure your object gets imported in this
    # function in order for it to be registered by services that may
    # need to receive it via RPC.
    __import__('t2stor.objects.volume')
    __import__('t2stor.objects.cluster')
    __import__('t2stor.objects.rpc_service')
    __import__('t2stor.objects.node')
