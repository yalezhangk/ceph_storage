#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from DSpace.objects.base import JsonEncoder


def register_all():
    # NOTE(danms): You must make sure your object gets imported in this
    # function in order for it to be registered by services that may
    # need to receive it via RPC.
    __import__('DSpace.objects.action_log')
    __import__('DSpace.objects.alert_log')
    __import__('DSpace.objects.alert_group')
    __import__('DSpace.objects.alert_rule')
    __import__('DSpace.objects.ceph_config')
    __import__('DSpace.objects.cluster')
    __import__('DSpace.objects.crush_rule')
    __import__('DSpace.objects.datacenter')
    __import__('DSpace.objects.disk')
    __import__('DSpace.objects.disk_partition')
    __import__('DSpace.objects.email_group')
    __import__('DSpace.objects.license')
    __import__('DSpace.objects.log_file')
    __import__('DSpace.objects.network')
    __import__('DSpace.objects.node')
    __import__('DSpace.objects.object_access_key')
    __import__('DSpace.objects.object_bucket')
    __import__('DSpace.objects.object_lifecycle')
    __import__('DSpace.objects.object_policy')
    __import__('DSpace.objects.object_user')
    __import__('DSpace.objects.osd')
    __import__('DSpace.objects.object_user')
    __import__('DSpace.objects.object_access_key')
    __import__('DSpace.objects.pool')
    __import__('DSpace.objects.radosgw')
    __import__('DSpace.objects.radosgw_router')
    __import__('DSpace.objects.radosgw_zone')
    __import__('DSpace.objects.router_service')
    __import__('DSpace.objects.rack')
    __import__('DSpace.objects.rpc_service')
    __import__('DSpace.objects.service')
    __import__('DSpace.objects.sysconfig')
    __import__('DSpace.objects.task')
    __import__('DSpace.objects.taskflow')
    __import__('DSpace.objects.user')
    __import__('DSpace.objects.volume')
    __import__('DSpace.objects.volume_access_path')
    __import__('DSpace.objects.volume_client')
    __import__('DSpace.objects.volume_client_group')
    __import__('DSpace.objects.volume_gateway')
    __import__('DSpace.objects.volume_mapping')
    __import__('DSpace.objects.volume_snapshot')


class _Json(object):
    def dumps(self, obj, *args, **kwargs):
        return json.dumps(obj, cls=JsonEncoder, *args, **kwargs)

    def loads(self, obj, *args, **kwargs):
        return json.loads(obj, *args, **kwargs)


Json = _Json()


def json_encode(obj):
    return Json.dumps(obj)
