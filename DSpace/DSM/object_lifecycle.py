from datetime import datetime

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.node import NodeTask
from DSpace.tools.s3 import S3Client

logger = logging.getLogger(__name__)
RGW_LIFECYCLE_TIME_KEY = 'rgw_lifecycle_work_time'

op_types = {
    "read": _("read"),
    "write": _("write"),
    "delete": _("delete"),
}


class ObjectLifecycleMixin(AdminBaseHandler):

    def check_object_storage_status(self, ctxt, bucket_id, op_type):
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        bucket_name = bucket.name
        if bucket.status != 'active':
            raise exception.Invalid(_('bucket: %s status abnormal') %
                                    bucket_name)
        user = objects.ObjectUser.get_by_id(ctxt, bucket.owner_id)

        # Check user status
        if user.status != 'active':
            raise exception.Invalid(_('obj_user: %s status abnormal') %
                                    user.display_name)
        # Check user op_mask
        op_mask = user.op_mask.split(',')
        if op_type not in op_mask:
            raise exception.OpMaskError(op_type=op_types[op_type])

        obj_access_keys = objects.ObjectAccessKeyList.get_all(
            ctxt, filters={'obj_user_id': user.id})
        if not obj_access_keys:
            raise exception.Invalid(_('obj_user: %s has not available '
                                      'access_key') % user.display_name)
        filters = {'status': s_fields.RadosgwStatus.ACTIVE}
        rgws = objects.RadosgwList.get_all(ctxt, filters=filters)
        if not rgws:
            raise exception.Invalid(_('has not available rgw'))
        return rgws[0], obj_access_keys[0], bucket

    def prepare_lifecycles_modify(self, ctxt, bucket_id, lifecycles):
        """check name and target

        :param ctxt: ctxt
        :param bucket_id: bucket ID
        :param lifecycles: a group lifecycle
        :return: db and s3_tool need datas
        """
        names = set()
        db_lifecycles = []
        s3_datas = []
        targets = []
        cluster_id = ctxt.cluster_id
        for lifecycle in lifecycles:
            name = lifecycle['name']
            target = lifecycle['target']
            if name in names:
                raise exception.Invalid(_('object_lifecycle %s repeat') % name)
            else:
                names.add(name)
                targets.append(str(target))
                lifecycle.update({
                    'bucket_id': bucket_id,
                    'cluster_id': cluster_id
                })
                db_lifecycles.append(lifecycle)
                expiration = lifecycle['policy']['expiration']
                if isinstance(expiration, int):
                    pass
                else:
                    expiration = datetime.strptime(expiration, '%Y-%m-%d')
                s3_data = {
                    'ID': name,
                    'Filter': lifecycle['target'],
                    'Status': 'Enabled' if lifecycle['enabled'] else
                              'Disabled',
                    'Expiration': expiration
                }
                s3_datas.append(s3_data)
        # target is prefix contain
        # sort排序，比较两个相邻的元素
        targets.sort()
        for i in range(len(targets)-1):
            is_contain = targets[i+1].startswith(targets[i])
            if is_contain:
                raise exception.Invalid(
                        _('lifecycle target contains: %s') % targets[i+1])
        return db_lifecycles, s3_datas

    def modify_object_lifecycle_by_db(self, ctxt, bucket, lifecycles):
        # delete old, create new lifecycles
        bucket_id = bucket.id
        del_filter = {'cluster_id': ctxt.cluster_id, 'bucket_id': bucket_id}
        del_lifecycles = objects.ObjectLifecycleList.get_all(
            ctxt, filters=del_filter)
        for lifecycle in del_lifecycles:
            lifecycle.destroy()
        for data in lifecycles:
            new_lifecycle = objects.ObjectLifecycle(ctxt, **data)
            new_lifecycle.create()
        logger.info('modify_object_lifecycle_by_db success, bucket_name=%s',
                    bucket.name)

    def object_lifecycle_get_default_work_time(self, ctxt):
        default_confs = self.get_default_ceph_confs()
        lifecycle = default_confs.get(RGW_LIFECYCLE_TIME_KEY)
        value = lifecycle.get('default')
        value_type = lifecycle.get('type')
        return value, value_type


class ObjectLifecycleHandler(ObjectLifecycleMixin):

    def object_lifecycle_get_all(self, ctxt, marker=None, limit=None,
                                 sort_keys=None, sort_dirs=None, filters=None,
                                 offset=None, expected_attrs=None):
        return objects.ObjectLifecycleList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def object_lifecycle_get_count(self, ctxt, filters=None):
        return objects.ObjectLifecycleList.get_count(ctxt, filters=filters)

    def object_lifecycle_modify(self, ctxt, data):
        bucket_id = data['bucket_id']
        rgw, access_key, bucket = self.check_object_storage_status(
            ctxt, bucket_id, 'write')
        bucket_name = bucket.name
        lifecycles = data['object_lifecycles']
        logger.debug('object_lifecycle begin modify, bucket_name=%s, data=%s',
                     bucket_name, lifecycles)
        db_datas, s3_datas = self.prepare_lifecycles_modify(
            ctxt, bucket_id, lifecycles)
        logger.info('prepare lifecycles_modify datas success, db_datas:%s,'
                    's3_datas:%s', db_datas, s3_datas)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.SET_LIFECYCLE)
        self.task_submit(self._object_lifecycle_modify, ctxt, rgw, access_key,
                         bucket, db_datas, s3_datas, begin_action)
        logger.debug('object_lifecycle_modify task has begin, bucket_name=%s, '
                     'prepared_data=%s', bucket_name, db_datas)
        return db_datas

    def _object_lifecycle_modify(self, ctxt, rgw, obj_access_key, bucket,
                                 db_datas, s3_datas, begin_action):
        endpoint_url = 'http://' + str(rgw.ip_address) + ':' + str(rgw.port)
        access_key = obj_access_key.access_key
        secret_key = obj_access_key.secret_key
        bucket_name = bucket.name
        try:
            s3_tool = S3Client(endpoint_url, access_key, secret_key)
            if s3_datas:
                s3_tool.bucket_lifecycle_set(bucket_name, s3_datas)
            else:
                s3_tool.bucket_lifecycle_clear(bucket_name)
            self.modify_object_lifecycle_by_db(ctxt, bucket, db_datas)
            logger.info('object_lifecycle_modify success, bucket_name=%s, '
                        'data=%s', bucket_name, db_datas)
            op_status = "SET_LIFECYCLE"
            msg = _("set lifecycle success, bucket_name:%s") % bucket_name
            err_msg = None
            status = 'success'
        except Exception as e:
            logger.exception('object_lifecycle_modify error, bucket_name=%s, '
                             'data=%s, reason:%s', bucket_name, db_datas,
                             str(e))
            op_status = "SET_LIFECYCLE"
            msg = _("set lifecycle error, bucket_name:%s") % bucket_name
            err_msg = str(e)
            status = 'fail'
        self.finish_action(begin_action, bucket.id, bucket_name, bucket,
                           status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, bucket, op_status, msg)

    def object_lifecycle_get_execute_time(self, ctxt):
        value = objects.ceph_config.ceph_config_get(
            ctxt, 'global', RGW_LIFECYCLE_TIME_KEY)
        default_time, *others = self.object_lifecycle_get_default_work_time(
            ctxt)
        return value if value else default_time

    def object_lifecycle_update_execute_time(self, ctxt, data):
        start_on = data.get('start_on')
        end_on = data.get('end_on')
        if start_on == end_on:
            raise exception.Invalid(_('start time must not equal end time '))
        work_time = start_on + '-' + end_on
        key = RGW_LIFECYCLE_TIME_KEY
        old_time = objects.ceph_config.ceph_config_get(ctxt, 'global', key)
        if work_time == old_time:
            raise exception.Invalid(_('work time not change'))
        ceph_lifecycle = objects.CephConfigList.get_all(
            ctxt, filters={'group': 'global', 'key': key})
        before_data = ceph_lifecycle[0] if ceph_lifecycle else None
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_LIFECYCLE, Action.UPDATE_WORK_TIME,
            before_data)
        if not before_data:
            logger.info('lifecycle_update_execute_time, db ceph_config has '
                        'not key: %s , will create', key)
            cephconf = objects.CephConfig(
                ctxt, group='global', key=key,
                value=work_time,
                value_type='string',
                cluster_id=ctxt.cluster_id
            )
            cephconf.create()
        else:
            cephconf = before_data
            cephconf.value = work_time
            cephconf.save()
        self.task_submit(self._object_lifecycle_update_execute_time, ctxt,
                         cephconf, work_time, old_time, begin_action)
        logger.debug('_object_lifecycle_update_execute_time task has begin, '
                     'work_time: %s', work_time)
        return cephconf

    def _object_lifecycle_update_execute_time(self, ctxt, cephconf, work_time,
                                              old_time, begin_action):
        rgw_nodes = self._get_rgw_node(ctxt, rgw_name='*')
        active_rgw = objects.RadosgwList.get_all(
            ctxt, filters={'status': s_fields.RadosgwStatus.ACTIVE})
        default_time, *others = self.object_lifecycle_get_default_work_time(
            ctxt)
        value = {
            'group': 'global',
            'key': RGW_LIFECYCLE_TIME_KEY,
            'value': work_time,
        }
        try:
            for node in rgw_nodes:
                node_task = NodeTask(ctxt, node)
                node_task.ceph_config_update(ctxt, value)
            for rgw in active_rgw:
                service_name = 'ceph-radosgw@rgw.{}'.format(rgw.name)
                node = objects.Node.get_by_id(ctxt, rgw.node_id)
                self.check_agent_available(ctxt, node)
                agent_client = self.agent_manager.get_client(node.id)
                agent_client.systemd_service_restart(ctxt, service_name)
            logger.info('object_lifecycle_update_execute_time success, '
                        'work_time: %s ', work_time)
            op_status = "LIFECYCLE_UPDATE_WORK_TIME_SUCCESS"
            msg = _("update lifecycle work_time success")
            err_msg = None
            status = 'success'
        except Exception as e:
            if old_time:
                cephconf.value = old_time
                cephconf.save()
            else:
                cephconf.value = default_time
                cephconf.save()
            logger.error('object_lifecycle_update_execute_time error, '
                         'now work_time: %s ', old_time if old_time else
                         default_time)
            op_status = "LIFECYCLE_UPDATE_WORK_TIME_ERROR"
            msg = _("update lifecycle work_time error")
            err_msg = str(e)
            status = 'fail'
        self.finish_action(begin_action, cephconf.id, cephconf.value, cephconf,
                           status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, cephconf, op_status, msg)
