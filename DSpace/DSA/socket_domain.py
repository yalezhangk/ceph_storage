#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
import queue
import socket

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.disk import DiskTool
from DSpace.tools.file import File as FileTool

logger = logging.getLogger(__name__)


class SocketDomainHandler(AgentBaseHandler):
    def __init__(self, *args, **kwargs):
        super(SocketDomainHandler, self).__init__(*args, **kwargs)
        self.ssh = self._get_executor()
        self.queue = queue.Queue()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if not self.sock:
            raise exception.StorException("socket get error: %s", self.sock)
        file_tool = FileTool(self.ssh)
        socket_file = file_tool.map(CONF.socket_file)
        dsa_lib_dir = file_tool.map(CONF.dsa_lib_dir)
        if not os.path.exists(dsa_lib_dir):
            os.mkdir(dsa_lib_dir)
        if os.path.exists(socket_file):
            os.unlink(socket_file)
        if self.sock.bind(socket_file):
            raise exception.StorException("socket bind error: %s", socket_file)
        self.task_submit(self._server_socket)
        self.task_submit(self._dispatch)

    def _server_socket(self):
        logger.info("Start unix socket domain")
        while True:
            data = self.sock.recv(512)
            logger.info("get udev event: %s", data)
            if not data:
                continue
            data = json.loads(data)
            self.queue.put(data)

    def _dispatch(self):
        logger.info("dispatch queue start")
        while True:
            data = self.queue.get()
            logger.info("start to execute udev event: %s", data)
            try:
                resource_type = data.get('resource_type')
                if resource_type == 'disk':
                    op = data.get('op')
                    slot = data.get('slot')
                    if op == 'online':
                        self.disk_online(slot=slot)
                    elif op == 'offline':
                        self.disk_offline(slot=slot)
                    else:
                        logger.info("Not support op: %s", op)
                elif resource_type == 'network':
                    op = data.get('op')
                    net = data.get('net')
                    if op == 'add':
                        self.network_add(net=net)
                    elif op == 'remove':
                        self.network_remove(net=net)
                    else:
                        logger.info("Not support op: %s", op)
                else:
                    logger.info("Not support resource type: %s", resource_type)
            except Exception as e:
                logger.exception("Failed execute op: %s", e)

    def disk_offline(self, slot):
        logger.info("disk remove, slot: %s", slot)
        self.admin.disk_offline(self.ctxt, slot, self.node.id)

    def disk_online(self, slot):
        logger.info("disk add, slot: %s", slot)
        executor = self._get_executor()
        disk_tool = DiskTool(executor)
        disk_info = disk_tool.get_disk_info_by_slot(slot)
        self.admin.disk_online(self.ctxt, disk_info, self.node.id)

    def network_remove(self, net):
        # TODO remove interface op
        pass

    def network_add(self, net):
        # TODO add interface op
        pass
