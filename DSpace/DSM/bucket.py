from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.tools.radosgw_admin import RadosgwAdmin
from DSpace.tools.s3 import S3Client

logger = logging.getLogger(__name__)


class BucketHandler(AdminBaseHandler):

    def object_bucket_get_all(
            self, ctxt, marker=None, limit=None,
            sort_keys=None, sort_dirs=None, filters=None,
            offset=None, expected_attrs=None):
        return objects.ObjectBucketList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def object_bucket_get_count(self, ctxt, filters=None):
        return objects.ObjectBucketList.get_count(ctxt, filters=filters)

    def _check_policy_exist(self, ctxt, data):
        policy = objects.ObjectPolicy.get_by_id(
                ctxt, data['policy_id'])
        if not policy:
            raise exception.InvalidInput(
                    _("Object policy placement do not exists"))
        logger.info('object_policy name %s', policy.name)
        placement = policy.name
        return placement

    def _check_user_exist(self, ctxt, data):
        users = objects.ObjectUser.get_by_id(
                ctxt, data['owner_id'])
        if not users:
            raise exception.InvalidInput(_("owner do not exists"))
        uid = users.uid
        max_buckets = users.max_buckets
        logger.info('owner name %s', users.uid)
        return uid, max_buckets

    def _check_server_exist(self, ctxt):
        server = objects.RadosgwList.get_all(
                 ctxt, filters={'status': s_fields.RadosgwStatus.ACTIVE})[0]
        if not server:
            raise exception.InvalidInput(_("rgw service do not exists"))
        return server

    def object_buckets_create(self, ctxt, data):
        placement = self._check_policy_exist(ctxt, data)
        server = self._check_server_exist(ctxt)
        uid, max_buckets = self._check_user_exist(ctxt, data)
        admin, access_key, secret_access_key = self.get_admin_user(ctxt)
        endpoint_url = str(server.ip_address) + ':' + str(server.port)
        rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)
        user_buckets_number = rgw.count_user_buckets(uid)
        logger.info('user buckets count %s', user_buckets_number)
        available_bucket_qua = max_buckets - user_buckets_number
        batch_create = data.get('batch_create')
        if batch_create:
            prefix = data.get('name')
            number = data.get('number')
            if number > available_bucket_qua:
                raise exception.InvalidInput(_("quota is not enough"))
            bucket_names = [prefix + str(i) for i in range(1, int(number)+1)]
            logger.debug('bucket begin create, name:%s' % bucket_names)
        else:
            bucket_names = [data.get('name')]
            if available_bucket_qua < 1:
                raise exception.InvalidInput(_("quota is not enough"))
            logger.debug('bucket begin create, name:%s' % bucket_names)
        for check_name in bucket_names:
            self._check_object_bucket_name_exist(ctxt, check_name)
        bucket_list = []
        for name in bucket_names:
            bucket_datas = {
                "name": name,
                "owner_id": data['owner_id'],
                "policy_id": data['policy_id'],
                "versioned": data['versioned'],
                "owner_permission": data['owner_permission'],
                "auth_user_permission": data['auth_user_permission'],
                "all_user_permission": data['all_user_permission'],
                "quota_max_size": data['quota_max_size'],
                "quota_max_objects": data['quota_max_objects'],
                "cluster_id": ctxt.cluster_id
                }
            logger.info("bucket data %s", bucket_datas)
            begin_action = self.begin_action(ctxt, Resource.OBJECT_BUCKET,
                                             Action.CREATE)
            bucket = objects.ObjectBucket(ctxt, **bucket_datas)
            bucket.status = s_fields.BucketStatus.ACTIVE
            bucket.create()
            self.task_submit(self._object_bucket_create, ctxt, bucket,
                             placement, rgw, endpoint_url, access_key,
                             secret_access_key, uid, begin_action)
            logger.info('bucket_create task has begin, bucket_name: %s',
                        name)
            bucket_list.append(bucket)
        return bucket_list

    def _check_object_bucket_name_exist(self, ctxt, name):
        exist_bucket = objects.ObjectBucketList.get_all(
            ctxt, filters={'name': name})
        if exist_bucket:
            raise exception.InvalidInput(
                reason=_('bucket name %s already exists!') % name)

    def _object_bucket_create(self, ctxt, bucket, placement, rgw,
                              endpoint_url, access_key, secret_access_key,
                              uid, begin_action):
        try:
            name = bucket.name
            acls = [{
                "Type": "CanonicalUser",
                "ID": uid,
                "Permission": bucket.owner_permission
            }]
            if bucket.auth_user_permission != '':
                auth_user_list = bucket.auth_user_permission.split(',')
                acls.append({
                        "Type": "Group",
                        "URI": "AuthenticatedUsers",
                        "Permission": auth_user_list[0]
                    })
                if len(auth_user_list) == 2:
                    add_auth_user_permission = {
                                "Type": "Group",
                                "URI": "AuthenticatedUsers",
                                "Permission": auth_user_list[1]
                            }
                    acls.append(add_auth_user_permission)
            if bucket.all_user_permission != '':
                all_user_list = bucket.all_user_permission.split(',')
                acls.append({
                        "Type": "Group",
                        "URI": "AllUsers",
                        "Permission": all_user_list[0]
                    })
                if len(all_user_list) == 2:
                    add_all_user_permission = {
                                "Type": "Group",
                                "URI": "AllUsers",
                                "Permission": all_user_list[1]
                            }
                    acls.append(add_all_user_permission)
            logger.info("acls info %s", acls)
            s3 = S3Client('http://' + endpoint_url,
                          access_key, secret_access_key)
            s3.bucket_create(name, placement=':' + placement,
                             acls=acls, versioning=bucket.versioned)
            msg = _("bucket %s create success") % name
            op_status = "CREATE_BUCKET_SUCCESS"
            if bucket.quota_max_size != 0 or bucket.quota_max_objects != 0:
                rgw.rgw.set_bucket_quota(
                        uid, name, bucket.quota_max_size,
                        bucket.quota_max_objects, enabled=True)
            bucket_info = rgw.rgw.get_bucket(bucket=name)
            bucket.bucket_id = bucket_info['id']
            bucket.save()
            rgw.bucket_owner_change(name, bucket_info['id'], uid)
            self.finish_action(begin_action, bucket.id, name,
                               bucket, 'success')
        except Exception as err:
            logger.info("create bucket err info %s", err)
            err_msg = str(err)
            msg = _("bucket %s create error") % name
            op_status = "CREATE_BUCKET_ERROR"
            self.finish_action(begin_action, bucket.id, name,
                               bucket, 'error', err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_multi_version():
        # TODO: multi_version
        pass

    def bucekt_access_permission():
        # TODO: access permission
        pass

    def bucket_quota_update():
        # TODO:
        pass

    def object_bucket_get(self, ctxt, object_bucket_id, expected_attrs=None):
        object_bucket = objects.ObjectPolicy.get_by_id(
            ctxt, object_bucket_id, expected_attrs=expected_attrs)
        return object_bucket

    def object_bucket_update(self, ctxt, object_bucket_id, data):
        logger.debug('object_bucket: %s begin update description',
                     object_bucket_id)
        bucket = objects.ObjectBucket.get_by_id(ctxt, object_bucket_id)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.UPDATE, bucket)
        bucket.save()
        logger.info('object_bucket update success, new_description=%s')
        self.finish_action(begin_action, object_bucket_id, bucket.name, bucket)
        return bucket

    def object_bucket_delete(self, ctxt, bucket_id):
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        if bucket.status not in [s_fields.BucketStatus.ACTIVE,
                                 s_fields.BucketStatus.CREATING,
                                 s_fields.BucketStatus.DELETING,
                                 s_fields.BucketStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " bucket can be delete"))
        logger.debug('object_bucket begin delete: %s', bucket.name)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.DELETE)
        bucket.status = s_fields.BucketStatus.DELETING
        bucket.destroy()
        self.task_submit(self._object_bucket_delete, ctxt, bucket,
                         begin_action)
        logger.info('object_bucket_delete task has begin, bucket_name: %s',
                    bucket.name)

    def _object_bucket_delete(self, ctxt, bucket, begin_action):
        self.finish_action(self, begin_action, bucket.id, bucket.name, bucket,
                           'success', err_msg=None)
        # send ws message
        # self.send_websocket(ctxt, bucket, op_status, msg)
        pass
