#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
        self.write(objects.json_encode({}))
