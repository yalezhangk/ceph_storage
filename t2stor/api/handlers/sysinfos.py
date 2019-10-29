#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.exception import InvalidInput
from t2stor.i18n import _

logger = logging.getLogger(__name__)


class SysInfoHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        sysconfs = yield client.sysconf_get_all(ctxt)
        self.write(json.dumps({
            "sysconf": {
                "cluster_name": sysconfs['cluster_name'],
                "admin_cidr": sysconfs['admin_cidr'],
                "public_cidr": sysconfs['public_cidr'],
                "cluster_cidr": sysconfs['cluster_cidr'],
                "gateway_cidr": sysconfs['gateway_cidr'],
                "chrony_server": sysconfs['chrony_server']
            }
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('data')
        logger.error(data)
        if not data:
            raise InvalidInput(reason=_("sysconf: post data is none"))
        gateway_cidr = data.get('gateway_cidr')
        cluster_cidr = data.get('cluster_cidr')
        public_cidr = data.get('public_cidr')
        admin_cidr = data.get('admin_cidr')
        chrony_server = data.get('chrony_server')
        cluster_name = data.get('cluster_name')

        client = self.get_admin_client(ctxt)

        if chrony_server:
            if len(str(chrony_server).split('.')) < 3:
                raise InvalidInput(reason=_("chrony_server is not a IP"))
            yield client.update_chrony(chrony_server)
        else:
            yield client.update_sysinfo(
                ctxt, cluster_name, admin_cidr, public_cidr,
                cluster_cidr, gateway_cidr)

        # TODO agent设置Chrony服务器
        return True


class SmtpHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        smtp_conf = yield client.smtp_get(ctxt)
        self.write(json.dumps({
            "smtp_conf": {
                "enabled": smtp_conf['enabled'],
                "smtp_user": smtp_conf['smtp_user'],
                "smtp_password": smtp_conf['smtp_password'],
                "smtp_host": smtp_conf['smtp_host'],
                "smtp_port": smtp_conf['smtp_port'],
                "enable_ssl": smtp_conf['enable_ssl'],
                "enable_tls": smtp_conf['enable_tls'],
            }
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('data')
        logger.error(data)
        if not data:
            raise InvalidInput(reason=_("smtp: post data is none"))
        enabled = data.get('enabled')
        smtp_user = data.get('smtp_user')
        smtp_password = data.get('smtp_password')
        smtp_host = data.get('smtp_host')
        smtp_port = data.get('smtp_port')
        enable_ssl = data.get('enable_ssl')
        enable_tls = data.get('enable_tls')
        client = self.get_admin_client(ctxt)
        yield client.update_smtp(
            ctxt, enabled, smtp_user, smtp_password,
            smtp_host, smtp_port, enable_ssl, enable_tls)
        self.write(json.dumps({'a': 'a'}))
