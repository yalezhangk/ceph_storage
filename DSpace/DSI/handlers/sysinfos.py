#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/sysinfos/")
class SysInfoHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - sysinfo
        summary: sysinfos List
        description: Return a list of sysinfos
        operationId: sysinfos.api.listSysinfo
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
        client = self.get_admin_client(ctxt)
        sysconfs = yield client.sysconf_get_all(ctxt)
        self.write(json.dumps({
            "sysconf": sysconfs
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - sysinfo
        summary: Create sysinfo
        description: Create sysinfo.
        operationId: nodes.api.createSysinfo
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
          name: sysinfo
          description: sysinfo's information
          required: true
          schema:
            type: object
            properties:
              data:
                type: object
                properties:
                  gateway_cidr:
                    type: string
                    description: gateway cidr
                  cluster_cidr:
                    type: string
                    description: cluster cidr
                  public_cidr:
                    type: string
                    description: public cidr
                  admin_cidr:
                    type: string
                    description: admin cidr
                  chrony_server:
                    type: string
                    description: chrony server
                  cluster_name:
                    type: string
                    description: cluster name
        "200":
          description: successful operation
        """
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
            yield client.update_chrony(ctxt, chrony_server)
        else:
            yield client.update_sysinfo(
                ctxt, cluster_name, admin_cidr, public_cidr,
                cluster_cidr, gateway_cidr)

        # TODO agent设置Chrony服务器


@URLRegistry.register(r"/sysconfs/smtp/")
class SmtpHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - smtp
        summary: smtp sysconfs
        description: Return a sysconfs of smtp
        operationId: smtp.api.sysconfsSmtp
        produces:
        - application/json
        parameters:
        - in: body
          name: smtp_conf
          description: Get smtp_sysconfs
          required: true
          schema:
            type: object
            properties:
              smtp_enabled:
                type: bool
                description: is or not smtp_enable
              smtp_user:
                type: string
                description: smtp_user_email
              smtp_password:
                type: string
                description: smtp_user_email_password
              smtp_host:
                type: string
                description: smtp_host
              smtp_port:
                type: string
                description: smtp_port
              smtp_enable_ssl:
                type: bool
                description: enable_ssl
              smtp_enable_tls:
                type: bool
                description: enable_tls
        responses:
        "200":
        description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        smtp_conf = yield client.smtp_get(ctxt)
        self.write(json.dumps({
            "smtp_conf": {
                "smtp_enabled": smtp_conf['smtp_enabled'],
                "smtp_user": smtp_conf['smtp_user'],
                "smtp_password": smtp_conf['smtp_password'],
                "smtp_host": smtp_conf['smtp_host'],
                "smtp_port": smtp_conf['smtp_port'],
                "smtp_enable_ssl": smtp_conf['smtp_enable_ssl'],
                "smtp_enable_tls": smtp_conf['smtp_enable_tls'],
            }
        }))

    @gen.coroutine
    def post(self):
        """
        Post smtp_sysconfs

        ---
        tags:
        - smtp
        summary: Post smtp_sysconfs
        description: Post smtp_sysconfs.
        operationId: sysconfs.api.postSmtp_sysconfs
        produces:
        - application/json
        parameters:
        - in: body
          name: data
          description: Post smtp_sysconfs
          required: true
          schema:
            type: object
            properties:
              smtp_enabled:
                type: bool
                description: is or not smtp_enable
              smtp_user:
                type: string
                description: smtp_user_email
              smtp_password:
                type: string
                description: smtp_user_email_password
              smtp_host:
                type: string
                description: smtp_host
              smtp_port:
                type: string
                description: smtp_port
              smtp_enable_ssl:
                type: bool
                description: enable_ssl
              smtp_enable_tls:
                type: bool
                description: enable_tls
        responses:
        "200":
        description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('data')
        logger.error(data)
        if not data:
            raise InvalidInput(reason=_("smtp: post data is none"))
        client = self.get_admin_client(ctxt)
        yield client.update_smtp(
            ctxt, data)
        self.write(json.dumps({'smtp_sysconfs': data}))


@URLRegistry.register(r"/sysconfs/mail/test/")
class SmtpTestHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - smtp
        summary: Post smtp_sysconfs
        description: Post smtp_sysconfs.
        operationId: sysconfs.api.postSmto_sysconfs
        produces:
        - application/json
        parameters:
        - in: body
         name: data
         description: Post smtp_sysconfs
         required: true
         schema:
           type: object
           properties:
             smtp_enabled:
               type: bool
               description: is or not smtp_enable
             smtp_user:
               type: string
               description: smtp_user_email
             smtp_password:
               type: string
               description: smtp_user_email_password
             smtp_host:
               type: string
               description: smtp_host
             smtp_port:
               type: string
               description: smtp_port
             smtp_enable_ssl:
               type: bool
               description: enable_ssl
             smtp_enable_tls:
               type: integer
               description: enable_tls
             smtp_subject:
               type: string
               description: test_mail_subject
             smtp_content:
               type: string
               description: test_mail_content
        responses:
        "200":
         description: successful operation
        """
        config = json_decode(self.request.body)
        config = config.get('smtp_conf')
        if not config:
            raise exception.NotFound(_("smtp could not be found."))
        subject = config.pop("smtp_subject")
        content = config.pop("smtp_content")
        if not content["smtp_subject"]:
            raise exception.NotFound(_("smtp_context could not be found."))
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        yield client.send_mail(subject, content, config)
        self.write(json.dumps({'result': 'true'}))
