#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import logging

import six
from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.objects import fields as s_fields
from DSpace.utils.license_verify import LicenseVerify
from DSpace.utils.security import check_encrypted_password

logger = logging.getLogger(__name__)

user_login_schema = {
    "type": "object",
    "properties": {
        "username": {
            "type": "string",
            "minLength": 5,
            "maxLength": 32
        },
        "password": {
            "type": "string",
        },
    },
    "required": ["username", "password"],
}

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


class PermissionMixin(BaseAPIHandler):
    @staticmethod
    def license_verify(ctxt, cluster_id=None):
        # license校验
        licenses = objects.LicenseList.get_latest_valid(ctxt)
        if not licenses:
            is_available = False
        else:
            v = LicenseVerify(licenses[0].content, ctxt)
            if not v.licenses_data:
                is_available = False
            else:
                if cluster_id:
                    # verify: expire,cluster_size,node_num
                    is_available = v.is_available()
                else:
                    # verify: expire
                    is_available = v.check_licenses_expiry()
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

    @gen.coroutine
    def check_init_page(self, ctxt):
        inited = objects.sysconfig.sys_config_get(
            ctxt, "platform_inited", default=False)
        if inited:
            return True
        logger.info("platform need init check")
        client = self.get_admin_client(ctxt)
        inited = yield client.cluster_platform_check(ctxt)
        logger.info("platform check value: %s", inited)
        if inited:
            logger.info("platform inited")
            objects.SysConfig(
                ctxt, key="platform_inited", value="True",
                value_type=s_fields.SysConfigType.BOOL,
                cluster_id=None
            ).create()
        return inited

    @gen.coroutine
    def get_permission(self, ctxt, user, cluster_id=None):
        permission = copy.deepcopy(default_permission)
        license = self.license_verify(ctxt, cluster_id)
        permission['license'] = license
        # platform_inited: true -> init done; false -> need init page
        permission['platform_inited'] = yield self.check_init_page(ctxt)
        permission['user'] = user
        if permission['platform_inited']:
            if cluster_id:
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


@URLRegistry.register(r"/users/")
class UserListHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - user
        summary: User List
        description: Return a list of users
        operationId: users.api.listuser
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: request
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        users = objects.UserList.get_all(ctxt, **page_args)
        user_count = objects.UserList.get_count(ctxt)
        self.write(objects.json_encode({
            "users": users,
            "total": user_count
        }))


@URLRegistry.register(r"/users/([0-9]*)/")
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


@URLRegistry.register(r"/login/")
class UserLoginHandler(PermissionMixin):
    def get_context(self):
        return RequestContext(user_id=None, is_admin=False)

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - user
        summary: user login
        description: User login.
        operationId: users.api.login
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: user
          description: user's information
          required: true
          schema:
            type: object
            properties:
              username:
                type: string
                description: user's name
              password:
                type: string
                description: user's password
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=user_login_schema,
                 format_checker=draft7_format_checker)
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
        permission = yield self.get_permission(ctxt, user)
        self.write(objects.json_encode(permission))


@URLRegistry.register(r"/logout/")
class UserLogoutHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - user
        summary: user logout
        description: User logout.
        operationId: users.api.logout
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        "200":
          description: successful operation
        """
        self.get_context()
        self.session['user'] = None
        self.write(objects.json_encode({}))


@URLRegistry.register(r"/permissions/")
class PermissionHandler(PermissionMixin):

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - user
        summary: return user's permission
        description: return user's permission.
        operationId: users.api.permission
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        permission = yield self.get_permission(
            ctxt, self.current_user,
            cluster_id=self.get_cluster_id())
        self.write(objects.json_encode(permission))
