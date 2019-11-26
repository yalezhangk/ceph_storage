import os

import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.node import NodeTask

logger = logging.getLogger(__name__)

OSD_ID_MAX = 1024 ^ 2
LOCAL_LOGFILE_DIR = '/var/log/dspace_log/'


class LogFileHandler(AdminBaseHandler):
    def log_file_get_all(self, ctxt, node_id, service_type, marker=None,
                         limit=None, sort_keys=None, sort_dirs=None,
                         filters=None, offset=None):
        # 1 参数校验，node/osd是否存在
        logger.debug('begin get log_file list')
        node = objects.Node.get_by_id(ctxt, node_id)
        if not node:
            raise exception.NodeNotFound(node_id=node_id)
        if service_type == s_fields.LogfileType.MON:
            if not node.role_monitor:
                raise exception.InvalidInput(
                    reason=_('node_id {} is not monitor role').format(node_id))
        elif service_type == s_fields.LogfileType.OSD:
            if not node.role_storage:
                raise exception.InvalidInput(
                    reason=_('node_id {} is not storage role').format(node_id))
        else:
            raise exception.InvalidInput(reason=_('service_type must is mon or'
                                                  'osd'))
        # 2 agent获取日志文件元数据
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=int(node_id))
        metadata = client.get_logfile_metadata(
            ctxt, node=node, service_type=service_type)
        if not metadata:
            logger.error('get log_file metadata error or without log_files')
            return None
        # 3 入库，删除旧的，再新增
        logger.info('get log_file metadata success')
        del_filter = {'cluster_id': ctxt.cluster_id, 'node_id': node_id,
                      'service_type': service_type}
        del_objs = objects.LogFileList.get_all(ctxt, filters=del_filter)
        for del_obj in del_objs:
            del_obj.destroy()
        result = []
        for per_log in metadata:
            filename = per_log['file_name']
            filesize = per_log['file_size']
            directory = per_log['directory']
            filters = {'cluster_id': ctxt.cluster_id, 'node_id': node_id,
                       'service_type': service_type, 'filename': filename,
                       'filesize': filesize, 'directory': directory}
            new_log = objects.LogFile(ctxt, **filters)
            new_log.create()
            result.append(new_log)
        logger.info('get log_file metadata success,'
                    'node_id:%s, service_type:%s', node_id, service_type)
        return result

    def log_file_get_count(self, ctxt, node_id, service_type, filters=None):
        filters = {
            'node_id': node_id,
            'service_type': service_type,
        }
        return objects.LogFileList.get_count(ctxt, filters=filters)

    def log_file_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_log = objects.LogFile(ctxt, **data)
        alert_log.create()
        return alert_log

    def log_file_get(self, ctxt, log_file_id):
        logger.debug('begin pull log_file, id:%s', log_file_id)
        # 参数校验
        log_file = objects.LogFile.get_by_id(ctxt, log_file_id)
        if not log_file:
            raise exception.LogFileNotFound(log_file_id=log_file_id)
        node = objects.Node.get_by_id(ctxt, log_file.node_id)
        directory = log_file.directory
        filename = log_file.filename
        # 拉取agent上文件到本机文件夹下
        if not os.path.exists(LOCAL_LOGFILE_DIR):
            os.makedirs(LOCAL_LOGFILE_DIR, mode=0o0755)
        try:
            task = NodeTask(ctxt, node)
            task.pull_logfile(directory, filename, LOCAL_LOGFILE_DIR)
        except exception.StorException as e:
            logger.exception('pull log_file error,%s', e)
            raise exception.CephException(message='pull log_file error')
        logger.info('pull log_file:%s success', log_file_id)
        return '{}{}'.format(LOCAL_LOGFILE_DIR, filename)

    def log_file_update(self, ctxt, log_file_id, data):
        log_file = self.log_file_get(ctxt, log_file_id)
        for k, v in six.iteritems(data):
            setattr(log_file, k, v)
        log_file.save()
        return log_file

    def log_file_delete(self, ctxt, log_file_id):
        log_file = self.log_file_get(ctxt, log_file_id)
        log_file.destroy()
        return log_file

    def download_log_file(self, ctxt, log_file_id):
        logger.debug('begin download_log_file, id:%s', log_file_id)
        # 1 参数校验
        log_file = objects.LogFile.get_by_id(ctxt, log_file_id)
        if not log_file:
            raise exception.LogFileNotFound(log_file_id=log_file_id)
        node_id = log_file.node_id
        node = objects.Node.get_by_id(ctxt, node_id)
        directory = log_file.directory
        filename = log_file.filename
        # 2 agent获取日志文件的base64的content
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=int(node_id))
        content = client.read_log_file_content(
            ctxt, node, directory, filename)
        logger.info('download_log_file success, id:%s', log_file_id)
        return {'file_name': filename, 'content': content}
