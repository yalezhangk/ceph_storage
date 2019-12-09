#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

logger = logging.getLogger(__name__)


class URLRegistry(object):
    _registry = None
    _routes = None

    def __new__(cls, *args, **kwargs):
        if not cls._registry:
            cls._registry = super(URLRegistry, cls).__new__(
                cls, *args, **kwargs)
        return cls._registry

    def __init__(self, *args, **kwargs):
        if self._routes is None:
            self._routes = []

    @classmethod
    def register(cls, url):
        registry = cls()

        def _wapper(handler):
            registry.register_url(url, handler)
            return handler
        return _wapper

    def register_url(self, url, handler):
        logger.info("register %s -> %s", url, handler)
        self._routes.append((url, handler))

    def routes(self):
        return self._routes


def get_routers():
    __import__('DSpace.DSI.handlers.disk_partitions')
    __import__('DSpace.DSI.handlers.action_log')
    __import__('DSpace.DSI.handlers.alert_group')
    __import__('DSpace.DSI.handlers.alert_log')
    __import__('DSpace.DSI.handlers.alert_rule')
    __import__('DSpace.DSI.handlers.ceph_config')
    __import__('DSpace.DSI.handlers.clusters')
    __import__('DSpace.DSI.handlers.components')
    __import__('DSpace.DSI.handlers.datacenters')
    __import__('DSpace.DSI.handlers.disks')
    __import__('DSpace.DSI.handlers.email_group')
    __import__('DSpace.DSI.handlers.licenses')
    __import__('DSpace.DSI.handlers.log_file')
    __import__('DSpace.DSI.handlers.networks')
    __import__('DSpace.DSI.handlers.nodes')
    __import__('DSpace.DSI.handlers.osds')
    __import__('DSpace.DSI.handlers.pools')
    __import__('DSpace.DSI.handlers.probe')
    __import__('DSpace.DSI.handlers.racks')
    __import__('DSpace.DSI.handlers.radosgw')
    __import__('DSpace.DSI.handlers.service')
    __import__('DSpace.DSI.handlers.rpc_service')
    __import__('DSpace.DSI.handlers.sysinfos')
    __import__('DSpace.DSI.handlers.tasks')
    __import__('DSpace.DSI.handlers.user')
    registry = URLRegistry()
    return registry.routes()
