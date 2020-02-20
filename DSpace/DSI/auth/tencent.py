#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import requests
from oslo_config import cfg
from requests.models import PreparedRequest
from tornado import gen

from DSpace import exception
from DSpace import objects
from DSpace.context import RequestContext
from DSpace.DSI.auth import AuthBackend
from DSpace.DSI.auth import AuthRegistry
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.exception import NotAuthorized
from DSpace.exception import NotFound

logger = logging.getLogger(__name__)
CONF = cfg.CONF
auth_opts = [
    cfg.StrOpt('sso_login_base',
               default=None,
               help="SSO login base endpoint"),
    cfg.StrOpt('sso_verificate',
               default=None,
               help="SSO verificate endpoint"),
]
CONF.register_opts(auth_opts)


class SSOEndopointNotProvided(NotFound):
    message = "SSO endpoint could not provided."


@AuthRegistry.register
class TencentTicket(AuthBackend):
    sso_login = None
    sso_verificate = None

    def __init__(self, *args, **kwargs):
        self.sso_login = CONF.sso_login_base
        if not self.sso_login:
            raise SSOEndopointNotProvided
        self.sso_verificate = CONF.sso_verificate
        if not self.sso_verificate:
            raise SSOEndopointNotProvided

    @classmethod
    def register_url(cls):
        URLRegistry.register("/login/")(UserLoginHandler)
        URLRegistry.register("/logout/")(UserLogoutHandler)

    @property
    def sso_login_url(self):
        return self.sso_login + "/login"

    @property
    def sso_logout_url(self):
        return self.sso_login + "/logout"

    @property
    def st_url(self):
        return self.sso_verificate + "/api/v1.0/auth/serviceValidate"

    @property
    def tgt_url(self):
        return self.sso_verificate + "/api/v1.0/auth/validate"

    def tgt_validate(self, tgt):
        """For all request"""
        data = {
            "ticket": tgt
        }
        r = requests.post(self.tgt_url, json=data)
        logger.info("tencent validate ticket: %s, code: %s data: %s",
                    tgt, r.status_code, r.json())
        if r.status_code != 200:
            raise NotAuthorized
        res = r.json()
        return res['data']

    def _exist_session(self, handler):
        ticket = handler.session['ticket']
        return ticket

    def _validate_session(self, handler):
        ticket = handler.session['ticket']
        logger.info("tencent validate ticket: %s", ticket)
        return self.tgt_validate(ticket)

    def _get_user_by_name(self, ctxt, username):
        users = objects.UserList.get_all(ctxt, filters={"name": username})
        if users:
            return users[0]
        raise exception.UserNotFound(user_id=username)

    def validate(self, ctxt, handler):
        if not self._exist_session(handler):
            raise NotAuthorized
        data = self._validate_session(handler)
        user = self._get_user_by_name(ctxt, data['user_name'])
        handler.current_user = user


class UserLoginHandler(BaseAPIHandler):
    def get_context(self):
        if not self.ctxt:
            self.ctxt = RequestContext(user_id="anonymous", is_admin=False)
            logger.debug("Context: %s", self.ctxt.to_dict())
        return self.ctxt

    def _get_or_create_user(self, ctxt, username):
        users = objects.UserList.get_all(ctxt, filters={"name": username})
        if users:
            user = users[0]
        else:
            user = objects.User(ctxt, name=username)
            user.create()
        return user

    def _validate_st(self, ctxt, ticket, service):
        params = {
            'ticket': ticket,
            'service': service,
        }
        logger.info("tencent validate st: %s, service: %s", ticket, service)
        r = requests.get(self.auth.st_url, params=params)
        logger.info("tencent validate st: %s, code: %s data: %s",
                    ticket, r.status_code, r.json())
        if r.status_code != 200:
            raise NotAuthorized
        res = r.json()
        data = res['data']
        self._get_or_create_user(ctxt, data['user_name'])
        self.session['username'] = data['user_name']
        self.session['ticket'] = data['ticket']

    def get(self):
        ctxt = self.get_context()
        ticket = self.get_argument("tstack_ticket", None)
        logger.info("tencent login st: %s", ticket)
        if ticket:
            service = self.session['service']
            self._validate_st(ctxt, ticket, service)
            callback = self.session['callback']
            if callback:
                self.redirect(callback)
        else:
            callback = self.get_argument("callback", None)
            prefix = self.get_argument("prefix", "")
            logger.info("tencent login cb: %s, prefix: ", callback, prefix)
            # sso_cb
            req = PreparedRequest()
            req.prepare_url(
                prefix + URLRegistry().get_url(UserLoginHandler),
                {}
            )
            sso_cb = req.url
            self.session['callback'] = callback
            self.session['service'] = sso_cb

            # login_url
            req = PreparedRequest()
            req.prepare_url(
                self.auth.sso_login_url,
                {"service": sso_cb})
            login_url = req.url

            logger.info("login_url: %s", login_url)
            self.write(objects.json_encode({
                "type": "External",
                "url": login_url
            }))


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

        "200":
          description: successful operation
        """
        self.get_context()
        self.session['username'] = None
        self.session['ticket'] = None

        # logout
        service = self.session['service']
        req = PreparedRequest()
        req.prepare_url(
            self.auth.sso_logout_url,
            {"service": service})
        logout_url = req.url
        self.write(objects.json_encode({
            "type": "External",
            "url": logout_url
        }))
