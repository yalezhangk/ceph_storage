#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

import six
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.context import RequestContext
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.utils.security import check_encrypted_password

logger = logging.getLogger(__name__)

fake = {
    "message": "ok",
    "code": 0,
    "data": {
        "license": True,
        "ws_info": {
            "url": "http://192.168.103.140:2080/",
            "path": "/ws/",
            "token": "TEcTfdRXUc4rhn79nOM6WSCj-DgTYXOFYvLBOAHAfKY"
        },
        "app": [
            {
                "app": "stor",
                "current_app": True
            }
        ],
        "user": {
            "mobile_phone": "11111111111",
            "name": "超级管理员",
            "email": "admin",
            "id": 1,
            "type": "CLOUD_ADMIN",
            "domain_id": 1,
            "uuid": "263c16f15a7342879029aba6ace89847"
        },
        "provider": [],
        "region": [],
        "permissions": {
            "stor": {
                "cluster": {
                    "collect": True
                },
                "manage-cluster": {
                    "collect": True
                },
                "object-storage": {
                    "collect": True
                },
                "download": {
                    "collect": True
                },
                "block_client": {
                    "collect": True
                },
                "object-router": {
                    "collect": True
                },
                "block_path": {
                    "collect": True
                },
                "cache": True,
                "storage": {
                    "collect": True
                },
                "alarm_center": {
                    "collect": True
                },
                "block_roll": {
                    "collect": True
                },
                "event_center": {
                    "collect": True
                },
                "settopology": {
                    "collect": True
                },
                "topology": {
                    "collect": True
                },
                "email_groups": {
                    "collect": True
                },
                "license": {
                    "collect": True
                },
                "server": {
                    "collect": True
                },
                "pools": {
                    "collect": True
                }
            }
        }
    }
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
            "total": len(user_count)
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


class UserLoginHandler(BaseAPIHandler):
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
        self.write(objects.json_encode(fake))


class UserLogoutHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        self.get_context()
        self.session['user'] = None
        self.write(objects.json_encode({}))


class PermissionHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        self.get_context()
        self.write(json.dumps(fake))
