#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import json
import os

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _
from DSpace.utils.license_verify import CA_FILE_PATH
from DSpace.utils.license_verify import PRIVATE_FILE
from DSpace.utils.license_verify import LicenseVerify

FILE_LEN = 2048


class LicenseHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        # unauthorized:未授权，authorized:已授权， lapsed:已失效
        ctxt = self.get_context()
        licenses = objects.LicenseList.get_latest_valid(ctxt)
        admin = self.get_admin_client(ctxt)
        cluster_info = yield admin.ceph_cluster_info(ctxt)
        size = int(cluster_info.get('total_cluster_byte', 0))
        nodes = yield admin.node_get_all(ctxt)
        result = {'license': []}
        if licenses:
            for per_license in licenses:
                v = LicenseVerify(per_license.content, ctxt)
                if not v.licenses_data:
                    result['license'].append({'status': 'unauthorized'})
                else:
                    is_available = v.is_available()
                    up_data = {
                        'id': per_license.id,
                        'status': 'authorized' if is_available else 'lapsed',
                        'not_before': v.not_before,
                        'not_after': v.not_after,
                        'product': 'T2STOR',
                        'fact_size': size,
                        'size': v.license_cluster_size,
                        'fact_node_num': len(nodes),
                        'node_num': v.licenses_node_number
                    }
                    result['license'].append(up_data)
        self.write(json.dumps(result))

    def post(self):
        ctxt = self.get_context()
        result = {'license': {'result': False, 'msg': None}}
        file_metas = self.request.files.get('file', None)
        if not file_metas:
            raise InvalidInput(reason=_('can not get file_metas'))
        byte_content = file_metas[0]['body']
        content = base64.b64encode(byte_content).decode('utf-8')
        if len(content) > FILE_LEN:
            raise InvalidInput(reason=_("File too large"))
        license = objects.License(ctxt, content=content, status='valid')
        # TODO license_verify
        # license_verify校验
        v = LicenseVerify(license.content, ctxt)
        if not v.licenses_data:
            license.status = 'invalid'
            raise InvalidInput(reason=_('the license.key is invalid'))
        else:
            result['license']['result'] = True
        license.create()
        self.write(json.dumps(result))


class DownloadlicenseHandler(BaseAPIHandler):

    def get(self):
        file_name = self.get_argument('file_name')
        if file_name == 'certificate.pem':
            file = {file_name: CA_FILE_PATH}
        elif file_name == 'private-key.pem':
            file = {file_name: PRIVATE_FILE}
        else:
            raise InvalidInput(reason=_('file_name not exist'))
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition',
                        'attachment; filename={}'.format(file_name))
        if not os.path.exists(file[file_name]):
            raise InvalidInput(reason=_('file not yet generate or '
                                        'file path is error'))
        with open(file[file_name], 'r') as f:
            file_content = f.read()
            self.write(file_content)
        self.finish()
