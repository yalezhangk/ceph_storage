#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import requests
from oslo_config import cfg

from DSpace import objects
from DSpace.DSI.auth import AuthBackend
from DSpace.DSI.auth import AuthRegistry
from DSpace.exception import NotFound
from DSpace.objects.fields import UserOriginType

logger = logging.getLogger(__name__)
CONF = cfg.CONF
auth_opts = [
    cfg.StrOpt('sso_base',
               default=None,
               help="SSO login base endpoint"),
    cfg.BoolOpt('sso_secure',
                default=True,
                help="If True endpoint will use https, else http"),
    cfg.BoolOpt('sso_verify',
                default=False,
                help="whether we verify the server's TLS certificate"),
]
CONF.register_opts(auth_opts)


class SSOEndopointNotProvided(NotFound):
    message = "SSO endpoint could not provided."


@AuthRegistry.register
class HciSSOAuth(AuthBackend):
    sso_base = None

    def __init__(self, *args, **kwargs):
        self.sso_base = CONF.sso_base
        if not self.sso_base:
            raise SSOEndopointNotProvided
        self.sso_verificate = CONF.sso_base
        if not self.sso_verificate:
            raise SSOEndopointNotProvided

    def auth_sso_base_url(self):
        if self.sso_verificate.endswith('/'):
            self.sso_verificate = self.sso_verificate[:-1]
        protocal = "https://" if CONF.sso_secure else "http://"
        return protocal + self.sso_verificate

    @property
    def sso_login_url(self):
        base_url = self.auth_sso_base_url()
        return base_url + "/portal/auth/login"

    @property
    def sso_check_url(self):
        base_url = self.auth_sso_base_url()
        return base_url + "/portal/api/"

    def validate_auth_info(self, handler, cookies_dict):
        # return False(auth_info error) or user_name(auth_info success)
        res = requests.get(self.sso_check_url, cookies=cookies_dict,
                           verify=CONF.sso_verify)
        if res.status_code != 200:
            logger.warning('validate_auth_info code: %s, will '
                           'redirect_url:%s', res.status_code,
                           self.sso_login_url)
            handler.redirect_url(self.sso_login_url)
            return False
        res_data = res.json()['data']
        user_name = res_data.get('user', {}).get('user_name')
        if not user_name:
            logger.warning('can not get hci current_user: %s' % user_name)
            handler.redirect_url(self.sso_login_url)
            return False
        permission = res_data.get('permissions', {}).get('admin')
        if not permission:
            # 不属于admin组
            logger.warning('hci_user: %s not permission' % user_name)
            handler.redirect_url(self.sso_login_url)
            return False
        logger.debug('validate hci user success: %s' % user_name)
        return user_name

    def _hci_validate(self, handler):
        # return False(validate error) or user_name(validate success)
        cookies = handler.cookies
        cookies_dict = {}
        for k, v in cookies.items():
            cookies_dict[k] = v.value
        return self.validate_auth_info(handler, cookies_dict)

    def _get_or_create_hci_user(self, ctxt, user_name):
        users = objects.UserList.get_all(
            ctxt, filters={"name": user_name, "origin": UserOriginType.HCI})
        if users:
            user = users[0]
        else:
            user = objects.User(ctxt, name=user_name,
                                origin=UserOriginType.HCI)
            user.create()
            logger.info('create hci user: %s success' % user_name)
        logger.debug('hci_user is: %s' % user_name)
        return user

    def validate(self, ctxt, handler):
        if not handler.cookies:
            redirect_url = self.sso_login_url
            logger.warning('request header not cookies, will redirect_url:%s'
                           % redirect_url)
            handler.redirect_url(redirect_url)
            # 认证未通过，validate函数结束
            return
        user_name = self._hci_validate(handler)
        if user_name is False:
            # 认证未通过，validate函数结束
            return
        user = self._get_or_create_hci_user(ctxt, user_name)
        handler.current_user = user
