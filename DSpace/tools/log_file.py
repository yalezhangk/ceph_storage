#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class LogFile(ToolBase):
    def get_logfile_metadata(self, service_type):
        cmd = 'stat -t /var/log/ceph/*{}*'.format(service_type)
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
            per_file['directory'] = '/var/log/ceph/'
            log_info_list.append(per_file)
        return log_info_list
