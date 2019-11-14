#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import logging

import six
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.utils.license_verify import LicenseVerify
from DSpace.utils.security import check_encrypted_password

logger = logging.getLogger(__name__)

default_permission = {
    "license": True,
    "ws_info": {
        "url": "http://%s:%s" % (CONF.my_ip, CONF.api_port),
        "path": "/api/ws/",
        "token": ""
    },
    "app": [
        {
            "app": "stor",
            "current_app": True
        }
    ],
    "user": None,
    "provider": [],
    "region": [],
    "permissions": {
        "stor": {
        }
    }
}


class PermissionMixin(object):
    @staticmethod
    def license_verify(ctxt):
        # license校验
        licenses = objects.LicenseList.get_latest_valid(ctxt)
        if not licenses:
            is_available = False
        else:
            v = LicenseVerify(licenses[0].content, ctxt)
            if not v.licenses_data:
                is_available = False
            else:
                is_available = v.is_available()
        return is_available

    def default_page(self):
        return [
            "cluster",
            "object-storage",
            "download",
            "block_client",
            "object-router",
            "block_path",
            "cache",
            "storage",
            "alarm_center",
            "block_roll",
            "event_center",
            "settopology",
            "topology",
            "email_groups",
            "license",
            "server",
            "cluster-plan",
            "pools",
        ]

    def add_page(self, permission, page):
        permission['permissions']['stor'][page] = True

    def check_init_page(self, ctxt):
        value = objects.sysconfig.sys_config_get(
            ctxt, "platform_init", default=False)
        return value

    def get_permission(self, ctxt, user, cluster_id=None):
        permission = copy.deepcopy(default_permission)
        license = self.license_verify(ctxt)
        permission['license'] = license
        # platform_init: true -> init done; false -> need init page
        permission['platform_init'] = self.check_init_page(ctxt)
        permission['user'] = user
        if permission['platform_init']:
            cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
            if cluster.is_admin:
                self.add_page(permission, "manage-cluster")
            for p in self.default_page():
                self.add_page(permission, p)
        else:
            self.add_page(permission, "set-system-info")

        # TODO: cache permission
        return {
            "message": "ok",
            "code": 0,
            "data": permission
        }


class UserListHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        users = objects.UserList.get_all(ctxt, **page_args)
        user_count = objects.UserList.get_count(ctxt)
        self.write(objects.json_encode({
            "users": users,
            "total": user_count
        }))


class UserHandler(BaseAPIHandler):
    @gen.coroutine
    def put(self, user_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('user')
        user = objects.User.get_by_id(ctxt, user_id)
        for k, v in six.iteritems(data):
            setattr(user, k, v)
        user.save()

        self.write(objects.json_encode({
            "user": user
        }))


class UserLoginHandler(BaseAPIHandler, PermissionMixin):
    def get_context(self):
        return RequestContext(user_id=None, is_admin=False)

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        name = data.get('username')
        password = data.get('password')
        users = objects.UserList.get_all(ctxt, filters={'name': name})
        if not users:
            raise exception.NotFound(user_id=name)
        user = users[0]
        r = check_encrypted_password(password, user.password)
        if not r:
            raise exception.PasswordError()
        self.session['user'] = user
        self.current_user = user
        ctxt.user_id = user.id
        permission = self.get_permission(ctxt, user)
        self.write(objects.json_encode(permission))


class UserLogoutHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        self.get_context()
        self.session['user'] = None
        self.write(objects.json_encode({}))


class PermissionHandler(BaseAPIHandler, PermissionMixin):

    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        permission = self.get_permission(ctxt, self.current_user,
                                         cluster_id=self.get_cluster_id())
        self.write(objects.json_encode(permission))
