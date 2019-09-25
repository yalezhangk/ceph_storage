#!/usr/bin/env python
# -*- coding: utf-8 -*-


def register_all():
    # NOTE(danms): You must make sure your object gets imported in this
    # function in order for it to be registered by services that may
    # need to receive it via RPC.
    __import__('stor.objects.volume')
    __import__('stor.objects.cluster')
    __import__('stor.objects.rpc_service')
