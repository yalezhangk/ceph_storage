from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.tools.radosgw_admin import RadosgwAdmin
from DSpace.tools.s3 import S3Client

logger = logging.getLogger(__name__)


class BucketHandler(AdminBaseHandler):

    def _check_policy_exist(self, ctxt, data):
        policy = objects.ObjectPolicy.get_by_id(
                ctxt, data['policy_id'])
        if not policy:
            raise exception.InvalidInput(
                    _("Object policy placement do not exists"))
        logger.info('object_policy name %s', policy.name)
        placement = policy.name
        return placement

    def _check_user_status(self, ctxt, owner_id, op_type):
        # Check user status
        filters = {'id': owner_id}
        users = objects.ObjectUserList.get_all(
                ctxt, filters=filters)
        if not users:
            raise exception.ObjectUserNotFound(object_user_id=owner_id)
        user = users[0]
        if user.status != s_fields.ObjectUserStatus.ACTIVE:
            raise exception.ObjectUserNotActive(user=user.display_name,
                                                status=user.status)
        # Check user op_mask
        op_mask = user.op_mask.split(',')
        if op_type not in op_mask:
            raise exception.OpMaskError(op_type=_(op_type))

        uid = user.uid
        max_buckets = users[0].max_buckets
        logger.info('owner name %s', users[0].uid)
        return uid, max_buckets

    def _check_active_rgw_exist(self, ctxt):
        rgws = objects.RadosgwList.get_all(
               ctxt, filters={'status': s_fields.RadosgwStatus.ACTIVE})
        if not rgws:
            return None
        return rgws[0]

    def _assert_rgw_active(self, ctxt):
        rgw = self._check_active_rgw_exist(ctxt)
        if rgw is None:
            raise exception.InvalidInput(_("rgw service do not exists"))
        return rgw

    def _get_bucket_owner_access_keys(self, ctxt, obj_user_id):
        accesskey = objects.ObjectAccessKeyList.get_all(
                ctxt, filters={'obj_user_id': obj_user_id})[0]
        if not accesskey:
            raise exception.InvalidInput(_("access key do not exists"))
        access_key = accesskey.access_key
        secret_access_key = accesskey.secret_key
        return access_key, secret_access_key

    def _add_acls(self, uid, data):
        acls = [{
            "Type": "CanonicalUser",
            "ID": uid,
            "Permission": data['owner_permission']
        }]
        if data['auth_user_permission'] != '':
            auth_per_list = data['auth_user_permission'].split(',')
            for auth_permission in auth_per_list:
                acls.append({
                        "Type": "Group",
                        "URI": "AuthenticatedUsers",
                        "Permission": auth_permission
                })
        if data['all_user_permission'] != '':
            auth_per_list = data['all_user_permission'].split(',')
            for auth_permission in auth_per_list:
                acls.append({
                        "Type": "Group",
                        "URI": "AllUsers",
                        "Permission": auth_permission
                })
        return acls

    def object_buckets_create(self, ctxt, data):
        logger.info("Create bucket with data: %s", data)
        # Create check
        placement = self._check_policy_exist(ctxt, data)
        rgw = self._assert_rgw_active(ctxt)
        # Get
        uid, max_buckets = self._check_user_status(
            ctxt, data['owner_id'], 'write')
        admin, access_key, secret_access_key = self.get_admin_user(ctxt)
        endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
        rgw_cli = RadosgwAdmin(access_key, secret_access_key, endpoint_url)

        user_access = objects.ObjectAccessKeyList.get_all(
                ctxt, filters={"obj_user_id": data["owner_id"]})
        access_key = user_access[0].access_key
        secret_access_key = user_access[0].secret_key

        user_buckets_number = rgw_cli.count_user_buckets(uid)
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
            bucket.status = s_fields.BucketStatus.CREATING
            bucket.create()
            self.task_submit(self._object_bucket_create, ctxt, bucket,
                             placement, rgw_cli, endpoint_url, access_key,
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

    def _object_bucket_create(self, ctxt, bucket, placement, rgw_cli,
                              endpoint_url, access_key, secret_access_key,
                              uid, begin_action):
        try:
            name = bucket.name
            data = {"owner_permission": bucket.owner_permission,
                    "auth_user_permission": bucket.auth_user_permission,
                    "all_user_permission": bucket.all_user_permission}
            acls = self._add_acls(uid, data)
            logger.info("acls info %s", acls)
            s3 = S3Client('http://' + endpoint_url,
                          access_key, secret_access_key)
            s3.bucket_create(name, placement=':' + placement,
                             acls=acls, versioning=bucket.versioned)
            if bucket.quota_max_size != 0 or bucket.quota_max_objects != 0:
                rgw_cli.rgw.set_bucket_quota(
                        uid, name, bucket.quota_max_size / 1024,
                        bucket.quota_max_objects, enabled=True)
            bucket_info = rgw_cli.rgw.get_bucket(bucket=name)
            bucket.bucket_id = bucket_info['id']
            bucket.status = 'active'
            msg = _("bucket %s create success") % name
            err_msg = None
            op_status = "CREATE_BUCKET_SUCCESS"
            status = 'success'
        except Exception as err:
            logger.error("create bucket err info %s", str(err))
            err_msg = str(err)
            bucket.status = 'error'
            msg = _("bucket %s create error") % name
            op_status = "CREATE_BUCKET_ERROR"
            status = 'error'
        bucket.save()
        bucket.used_capacity_quota = 0
        bucket.used_object_quota = 0
        bucket.policy = objects.ObjectPolicy.get_by_id(
                ctxt, bucket.policy_id)
        bucket.owner = objects.ObjectUser.get_by_id(
                ctxt, bucket.owner_id)
        logger.info("bucket info %s", bucket)
        self.finish_action(begin_action, bucket.id, name,
                           bucket, status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_update_quota(self, ctxt, bucket_id, data):
        rgw = self._assert_rgw_active(ctxt)
        logger.debug('bucket quota: %s update begin', bucket_id)
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.QUOTA_UPDATE, bucket)
        self.task_submit(self._bucket_update_quota, ctxt, bucket, data,
                         rgw, begin_action)
        return bucket

    def _bucket_update_quota(self, ctxt, bucket, data, rgw,
                             begin_action):
        try:
            quota_max_size = data['quota_max_size']
            quota_max_objects = data['quota_max_objects']
            name = bucket.name
            users = objects.ObjectUser.get_by_id(ctxt, bucket.owner_id)
            uid = users.uid
            endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
            admin, access_key, secret_access_key = self.get_admin_user(ctxt)
            rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)
            rgw.rgw.set_bucket_quota(
                    uid, name, quota_max_size / 1024,
                    quota_max_objects, enabled=True)
            bucket.quota_max_size = data['quota_max_size']
            bucket.quota_max_objects = data['quota_max_objects']
            bucket.save()
            status = "success"
            op_status = "UPDATE_BUCKET_QUOTA_SUCCESS"
            msg = _("bucket {} update quota success").format(name)
            err_msg = None
        except Exception as err:
            logger.error("update bucket quota error: %s", err)
            msg = _("bucket {} update quota error").format(name)
            err_msg = str(err)
            status = "error"
            op_status = "UPDATE_BUCKET_QUOTA_ERROR"
        self.finish_action(begin_action, bucket.id, name, bucket,
                           status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def object_bucket_delete(self, ctxt, bucket_id, force):
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        rgw = self._assert_rgw_active(ctxt)
        admin, access_key, secret_access_key = self.get_admin_user(ctxt)
        endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
        rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)

        # Get all bucket list in rgw
        buckets = rgw.rgw.get_bucket()
        bucket_in_rgw = False
        if bucket.name in buckets:
            bucket_in_rgw = True
            bucket_info = rgw.rgw.get_bucket(bucket=bucket.name, stats=True)
            capacity = rgw.get_bucket_capacity(bucket_info)
            if capacity['size']['used'] != 0 and not force:
                raise InvalidInput(_("bucket {} need to "
                                     "be forced to delete").format(
                    bucket.name))

        if bucket.status not in [s_fields.BucketStatus.ACTIVE,
                                 s_fields.BucketStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " bucket can be delete"))
        logger.debug('object_bucket begin delete: %s', bucket.name)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.DELETE)
        bucket.status = s_fields.BucketStatus.DELETING
        bucket.save()
        self.task_submit(self._object_bucket_delete, ctxt, bucket,
                         rgw, force, bucket_in_rgw, begin_action)
        logger.info('object_bucket_delete task has begin, bucket_name: %s',
                    bucket.name)
        return bucket

    def _object_bucket_delete(self, ctxt, bucket, rgw, force, bucket_in_rgw,
                              begin_action):
        name = bucket.name
        try:
            if bucket_in_rgw:
                rgw.bucket_remove(name, force)
            bucket.destroy()
            status = "success"
            op_status = "DELETE_BUCKET_SUCCESS"
            msg = _("delete bucket {} success").format(name)
            err_msg = None
        except Exception as err:
            logger.error("delete bucket error: %s", err)
            bucket.status = s_fields.BucketStatus.ERROR
            bucket.save()
            msg = _("delete bucket {} error").format(name)
            err_msg = str(err)
            status = "error"
            op_status = "DELETE_BUCKET_ERROR"
        self.finish_action(begin_action, bucket.id, name, bucket,
                           status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_get_all(self, ctxt, tab, marker=None, limit=None,
                       sort_keys=None, sort_dirs=None, filters=None,
                       offset=None, expected_attrs=None):
        buckets = objects.ObjectBucketList.get_all(
                ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
                sort_dirs=sort_dirs, filters=filters, offset=offset,
                expected_attrs=expected_attrs)
        if not buckets:
            return []
        if tab == "default":
            rgw = self._check_active_rgw_exist(ctxt)
            if rgw:
                buckets = self.get_quota_objects(ctxt, rgw, buckets)
            for bucket in buckets:
                bucket.metrics = self.object_bucket_bandwidth_total(
                    ctxt, bucket)
        elif tab == 'io':
            for bucket in buckets:
                bucket.metrics = self.object_bucket_metrics_get(
                    ctxt, bucket.id)
        return buckets

    def get_quota_objects(self, ctxt, rgw, buckets):
        admin, access_key, secret_access_key = self.get_admin_user(ctxt)
        endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
        rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)
        bucket_infos = rgw.rgw.get_bucket(stats=True)
        bucket_infos = {bucket_info['bucket']: bucket_info
                        for bucket_info in bucket_infos}
        for bucket in buckets:
            bucket_info = bucket_infos.get(bucket.name)
            if not bucket_info:
                continue
            capacity = rgw.get_bucket_capacity(bucket_info)
            capacity_quota = capacity["size"]
            object_quota = capacity["objects"]
            bucket.used_capacity_quota = capacity_quota["used"]
            bucket.used_object_quota = object_quota["used"]
        return buckets

    def bucket_get_count(self, ctxt, filters=None):
        return objects.ObjectBucketList.get_count(ctxt, filters=filters)

    def bucket_get(self, ctxt, bucket_id, expected_attrs):
        bucket = objects.ObjectBucket.get_by_id(
            ctxt, bucket_id, expected_attrs)
        bucket.metrics = self.object_bucket_bandwidth_total(ctxt, bucket)
        return bucket

    def bucket_update_owner(self, ctxt, bucket_id, data):
        rgw = self._assert_rgw_active(ctxt)
        uid, max_buckets = self._check_user_status(
            ctxt, data['owner_id'], 'write')
        logger.debug('object_bucket owner: %s update begin',
                     bucket_id)
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, Action.OWNER_UPDATE, bucket)
        self.task_submit(self._bucket_update_owner, ctxt, bucket, data,
                         rgw, uid, begin_action)
        return bucket

    def _bucket_update_owner(self, ctxt, bucket, data, rgw,
                             uid, begin_action):
        try:
            name = bucket.name
            endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
            admin, access_key, secret_access_key = self.get_admin_user(ctxt)
            rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)
            rgw.bucket_owner_change(name, bucket['bucket_id'], uid)
            bucket.owner_id = data['owner_id']
            bucket.save()
            status = "success"
            op_status = "UPDATE_BUCKET_OWNER_SUCCESS"
            msg = _("bucket {} update owner success").format(name)
            err_msg = None
        except Exception as err:
            logger.error("bucket update owner error: %s", err)
            msg = _("bucket {} update owner error").format(name)
            err_msg = str(err)
            status = "error"
            op_status = "UPDATE_BUCKET_OWNER_ERROR"
        self.finish_action(begin_action, bucket.id, name, bucket,
                           status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_update_access_control(self, ctxt, bucket_id, data):
        rgw = self._assert_rgw_active(ctxt)
        logger.debug('bucket access control: %s update begin', bucket_id)
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET,
            Action.ACCESS_CONTROL_UPDATE, bucket)
        self.task_submit(self._bucket_update_access_control, ctxt, bucket,
                         data, rgw, begin_action)
        return bucket

    def _bucket_update_access_control(self, ctxt, bucket, data, rgw,
                                      begin_action):
        users = objects.ObjectUser.get_by_id(ctxt, bucket.owner_id)
        uid = users.uid
        try:
            acls = self._add_acls(uid, data)
            name = bucket.name
            endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
            obj_user_id = bucket.owner_id
            access_key, secret_access_key = \
                self._get_bucket_owner_access_keys(ctxt, obj_user_id)
            s3 = S3Client('http://' + endpoint_url,
                          access_key, secret_access_key)
            s3.bucket_acl_set(name, acls)
            bucket.auth_user_permission = data['auth_user_permission']
            bucket.all_user_permission = data['all_user_permission']
            bucket.save()
            status = "success"
            op_status = "UPDATE_BUCKET_ACCESS_CONTROL_SUCCESS"
            msg = _("bucket {} update access control success").format(name)
            err_msg = None
        except Exception as err:
            logger.error("bucket update access control error: %s", err)
            status = "error"
            op_status = "UPDATE_BUCKET_ACCESS_CONTROL_ERROR"
            msg = _("bucket {} update access control error").format(name)
            err_msg = str(err)
        self.finish_action(begin_action, bucket.id, name, bucket,
                           status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_versioning_update(self, ctxt, bucket_id, data):
        rgw = self._assert_rgw_active(ctxt)
        logger.debug('bucket versioning: %s update begin', bucket_id)
        action = Action.UPDATE_VERSIONING_OPEN if data['versioned'] \
            else Action.UPDATE_VERSIONING_SUSPENDED
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        self._check_user_status(ctxt, bucket.owner_id, 'write')
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_BUCKET, action, bucket)
        self.task_submit(self._bucket_versioning_update, ctxt, bucket,
                         data, rgw, begin_action)
        return bucket

    def _bucket_versioning_update(self, ctxt, bucket, data,
                                  rgw, begin_action):
        version_status = 'open' if data['versioned'] else 'suspended'
        name = bucket.name
        try:
            endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
            obj_user_id = bucket.owner_id
            access_key, secret_access_key = \
                self._get_bucket_owner_access_keys(ctxt, obj_user_id)
            s3 = S3Client('http://' + endpoint_url,
                          access_key, secret_access_key)
            s3.bucket_versioning_set(name, enabled=data['versioned'])
            bucket.versioned = data['versioned']
            bucket.save()
            status = "success"
            op_status = "OPEN_BUCKET_VERSIONING_SUCCESS" if \
                data['versioned'] else "SUSPENDED_BUCKET_VERSIONING_SUCCESS"
            if version_status == 'open':
                msg = _("bucket {} open versioning success").format(name)
            elif version_status == 'suspended':
                msg = _("bucket {} suspended versioning success").format(name)
            err_msg = None
        except Exception as err:
            logger.error("bucket update versioning error: %s", err)
            status = "error"
            op_status = "OPEN_BUCKET_VERSIONING_ERROR" if \
                data['versioned'] else "SUSPENDED_BUCKET_VERSIONING_ERROR"
            if version_status == 'open':
                msg = _("bucket {} open versioning error").format(name)
            elif version_status == 'suspended':
                msg = _("bucket {} suspended versioning error").format(name)
            err_msg = str(err)
        self.finish_action(begin_action, bucket.id, name, bucket,
                           status, err_msg=err_msg)
        self.send_websocket(ctxt, bucket, op_status, msg)

    def bucket_get_capacity(self, ctxt, bucket_id):
        rgw = self._assert_rgw_active(ctxt)
        bucket = objects.ObjectBucket.get_by_id(ctxt, bucket_id)
        name = bucket.name
        admin, access_key, secret_access_key = self.get_admin_user(ctxt)
        endpoint_url = str(rgw.ip_address) + ':' + str(rgw.port)
        rgw = RadosgwAdmin(access_key, secret_access_key, endpoint_url)
        bucket_info = rgw.rgw.get_bucket(bucket=name, stats=True)
        capacity = rgw.get_bucket_capacity(bucket_info)
        max_objects = capacity['objects']['max']
        max_size = capacity['size']['max']
        if max_objects == -1:
            capacity['objects']['max'] = 0
        if max_size == -1:
            capacity['size']['max'] = 0
        return capacity
