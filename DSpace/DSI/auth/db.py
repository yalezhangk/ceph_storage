#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.context import RequestContext
from DSpace.DSI.auth import AuthBackend
from DSpace.DSI.auth import AuthRegistry
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.exception import NotAuthorized
from DSpace.utils.security import check_encrypted_password

logger = logging.getLogger(__name__)

user_login_schema = {
    "type": "object",
    "properties": {
        "username": {
            "type": "string"
        },
        "password": {
            "type": "string",
        },
    },
    "required": ["username", "password"],
}


@AuthRegistry.register
class DBAuth(AuthBackend):

    @classmethod
    def register_url(cls):
        URLRegistry.register("/login/")(UserLoginHandler)
        URLRegistry.register("/logout/")(UserLogoutHandler)

    def _exist_session(self, handler):
        user_id = handler.session['user_id']
        logger.info("session user_id: %s", user_id)
        return user_id

    def validate(self, handler):
        if not self._exist_session(handler):
            raise NotAuthorized
        user = objects.User(id=handler.session['user_id'],
                            name=handler.session['username'])
        handler.current_user = user


class UserLoginHandler(BaseAPIHandler):
    def get_context(self):
        return RequestContext(user_id=None, is_admin=False)

    def get_first_cluster_id(self, ctxt):
        clusters = objects.ClusterList.get_all(ctxt, limit=1)
        if clusters:
            return clusters[0].id
        else:
            return None

    @gen.coroutine
    def get(self):
        self.write(objects.json_encode({
            "type": "Build-in"
        }))

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
            raise exception.UserNotFound(user_id=name)
        user = users[0]
        ctxt.user_id = user.id
        if not user.current_cluster_id:
            # 初次登录绑定一个cluster_id
            first_clu_id = self.get_first_cluster_id(ctxt)
            user.current_cluster_id = first_clu_id
            user.save()
        r = check_encrypted_password(password, user.password)
        if not r:
            raise exception.PasswordError()
        self.session['user_id'] = user.id
        self.session['username'] = user.name
        self.current_user = user
        self.write(objects.json_encode(user))


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

        "200":
          description: successful operation
        """
        self.get_context()
        self.session['user_id'] = None
        self.session['user_name'] = None
        self.write(objects.json_encode({
            "type": "Build-in",
        }))
