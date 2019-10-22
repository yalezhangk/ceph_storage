#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import json

from t2stor import objects
from t2stor.api.handlers.base import BaseAPIHandler
from t2stor.exception import InvalidInput
from t2stor.i18n import _
from t2stor.utils.license_verify import CA_FILE_PATH
from t2stor.utils.license_verify import PRIVATE_FILE
from t2stor.utils.license_verify import LicenseVerify

FILE_LEN = 2048


class LicenseHandler(BaseAPIHandler):

    def get(self):
        # unauthorized:未授权，authorized:已授权， lapsed:已失效
        ctxt = self.get_context()
        licenses = objects.LicenseList.get_latest_valid(ctxt)
        result = {'license': []}
        if licenses:
            for per_license in licenses:
                v = LicenseVerify(per_license.content)
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
                        'fact_size': v.fact_cluster_size,
                        'size': v.license_cluster_size,
                        'fact_node_num': v.fact_node_num,
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
        # license_verify校验
        v = LicenseVerify(license.content)
        if not v.licenses_data:
            license.status = 'invalid'
            raise InvalidInput(reason=_('the license.key is not invalid'))
        else:
            result['license']['result'] = True
        license.create()
        self.write(json.dumps(result))


class DownloadlicenseHandler(BaseAPIHandler):

    def get(self):
        file_name = self.get_argument('file_name')
        if file_name == 'certificate_file':
            file = {file_name: CA_FILE_PATH}
        elif file_name == 'private_file':
            file = {file_name: PRIVATE_FILE}
        else:
            raise InvalidInput(reason=_('file_name not exist'))
        with open(file[file_name], 'r') as f:
            file_content = f.read()
            self.write(file_content)
        self.finish()
