#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import logging
import os

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSM.base import AdminBaseHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.utils.license_verify import CA_FILE_PATH
from DSpace.utils.license_verify import PRIVATE_FILE

FILE_LEN = 2048
logger = logging.getLogger(__name__)


@URLRegistry.register(r"/licenses/")
class LicenseHandler(BaseAPIHandler):

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - liences
        summary: return the liences information
        description: return the liences information
        operationId: liences.api.getLiences
        produces:
        - application/json
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        result = yield client.license_get_all(ctxt)
        self.write(objects.json_encode(result))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - liences
        summary: upload the liences file
        description: upload liences file
        operationId: liences.api.uploadLiences
        produces:
        - application/json
        parameters:
        - in: request
          name: liences file
          description: liences file
          type: file
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        file_metas = self.request.files.get('file', None)
        if not file_metas:
            raise InvalidInput(reason=_('can not get file_metas'))
        byte_content = file_metas[0]['body']
        content = base64.b64encode(byte_content).decode('utf-8')
        if len(content) > FILE_LEN:
            raise InvalidInput(reason=_("File too large"))
        client = self.get_admin_client(ctxt)
        result = yield client.upload_license(ctxt, content)
        self.write(objects.json_encode(result))


@URLRegistry.register(r"/licenses/download_file/")
class DownloadlicenseHandler(BaseAPIHandler, AdminBaseHandler):

    def get(self):
        """
        ---
        tags:
        - liences
        summary: download the liences file
        description: download liences file by file name
        operationId: liences.api.downloadLiences
        produces:
        - application/json
        parameters:
        - in: request
          name: file_name
          description: liences file's name, it can be
                       certificate.pem/private-key.pem
          type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        file_name = self.get_argument('file_name')
        cluster_id = self.get_argument('cluster_id', None)
        if not cluster_id:
            raise InvalidInput(_('cluster_id is required'))
        ctxt.cluster_id = cluster_id
        begin_action = self.begin_action(
            ctxt, Resource.LICENSE, Action.DOWNLOAD_LICENSE)
        if file_name == 'certificate.pem':
            file = {file_name: CA_FILE_PATH}
        elif file_name == 'private-key.pem':
            file = {file_name: PRIVATE_FILE}
        else:
            err_msg = _('file_name not exist')
            self.finish_action(begin_action, None, file_name,
                               status='fail', err_msg=err_msg)
            raise InvalidInput(reason=err_msg)
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition',
                        'attachment; filename={}'.format(file_name))
        if not os.path.exists(file[file_name]):
            err_msg = _('file not yet generate or file path is error')
            self.finish_action(begin_action, None, file_name,
                               status='fail', err_msg=err_msg)
            raise InvalidInput(reason=err_msg)
        with open(file[file_name], 'r') as f:
            file_content = f.read()
            self.write(file_content)
        self.finish_action(begin_action, None, file_name, status='success')
        self.finish()
