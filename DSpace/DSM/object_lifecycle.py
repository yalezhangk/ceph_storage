from datetime import datetime

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.tools.s3 import S3Client

logger = logging.getLogger(__name__)


class ObjectLifecycleMixin(AdminBaseHandler):

    def check_object_storage_status(self, ctxt, bucket_id):
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        bucket_name = bucket.name
        if bucket.status != 'active':
            raise exception.Invalid(_('bucket: %s status abnormal') %
                                    bucket_name)
        user = objects.ObjectUser.get_by_id(ctxt, bucket.owner_id)
        if user.status != 'active':
            raise exception.Invalid(_('obj_user: %s status abnormal') %
                                    user.display_name)
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
            ctxt, bucket_id)
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
        self.send_websocket(ctxt, db_datas, op_status, msg,
                            resource_type=Resource.OBJECT_BUCKET)
