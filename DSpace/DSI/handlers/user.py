#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import ConfigKey
from DSpace.utils.license_verify import LicenseVerify
from DSpace.utils.security import check_encrypted_password

logger = logging.getLogger(__name__)


user_password_schema = {
    "type": "object",
    "properties": {
        "password": {
            "type": "string",
        },
        "new_password": {
            "type": "string",
            "minLength": 5,
            "maxLength": 32
        },
    },
    "required": ["password", "new_password"],
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
        disable_license = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DISABLE_LICENSE)
        if disable_license and (disable_license is True):
            logger.info('skip license verify, return default true')
            return True
        licenses = objects.LicenseList.get_all(ctxt)
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
            "object-storage-gateways",
            "download",
            "volume-client-groups",
            "object-router",
            "object-user",
            "object-bucket",
            "object-policy",
            "volume-access-paths",
            "caches",
            "osds",
            "alert-center",
            "volumes",
            "event-center",
            "configs",
            "topology",
            "email-groups",
            "license",
            "servers",
            "cluster-plan",
            "pools",
        ]

    def import_page(self):
        return [
            "cluster",
            "object-storage-gateways",
            "download",
            "object-router",
            "caches",
            "osds",
            "alert-center",
            "event-center",
            "configs",
            "topology",
            "email-groups",
            "license",
            "servers",
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
                value_type=s_fields.ConfigType.BOOL,
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
            include_tag = objects.sysconfig.sys_config_get(ctxt, 'is_import')
            if include_tag:
                pages = self.import_page()
            else:
                pages = self.default_page()
            for p in pages:
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


@URLRegistry.register(r"/users/password/")
class UserHandler(BaseAPIHandler):
    @gen.coroutine
    def put(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('user')
        validate(data, schema=user_password_schema,
                 format_checker=draft7_format_checker)
        user = objects.User.get_by_id(ctxt, ctxt.user_id)
        password = data.get('password')
        new_password = data.get('new_password')
        if password == new_password:
            raise exception.InvalidInput(
                _("New password not allow equal to old password"))
        r = check_encrypted_password(password, user.password)
        if not r:
            raise exception.PasswordError()
        user.password = new_password
        user.save()

        self.write(objects.json_encode({
            "user": user
        }))


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
        user = self.current_user
        permission = yield self.get_permission(
            ctxt, user,
            cluster_id=self.get_cluster_id())
        self.write(objects.json_encode(permission))
