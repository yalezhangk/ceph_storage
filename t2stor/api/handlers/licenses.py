#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import json

from t2stor import objects
from t2stor.api.handlers.base import BaseAPIHandler
from t2stor.exception import InvalidInput
from t2stor.i18n import _
from t2stor.utils.license_verify import LicenseVerify

FILE_LEN = 2048


class LicenseHandler(BaseAPIHandler):

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
