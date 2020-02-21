#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import logging
import os

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)
CEPH_LOG_DIR = '/var/log/ceph/'


class LogFile(ToolBase):
    def get_logfile_metadata(self, service_type):
        file_dir = '{}*{}*'.format(CEPH_LOG_DIR, service_type)
        cmd = 'stat -t {}'.format(file_dir)
        code, out, err = self.run_command(cmd)
        if code:
            logger.error('get_log_file_metadata error,out:%s', out)
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        ssh_result = out.split('\n')[:-1]
        log_info_list = []
        for one_file in ssh_result:
            per_file = {}
            file_info = one_file.strip().split(' ')
            per_file['file_name'] = file_info[0].split('/')[-1]
            per_file['file_size'] = file_info[1]
            per_file['directory'] = CEPH_LOG_DIR
            log_info_list.append(per_file)
        return log_info_list

    def read_log_file_content(self, directory, filename, offset, length):
        file_path = self._wapper('{}{}'.format(directory, filename))
        with open(file_path, 'rb') as file:
            file.seek(offset, os.SEEK_SET)
            con_byte = file.read(length)
            content = base64.b64encode(con_byte).decode('utf-8')
        return content

    def log_file_size(self, directory, filename):
        file_path = self._wapper('{}{}'.format(directory, filename))
        file_size = os.path.getsize(file_path)
        return file_size
