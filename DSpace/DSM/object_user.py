from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.tools.radosgw_admin import RadosgwAdmin

logger = logging.getLogger(__name__)


class ObjectUserMixin(AdminBaseHandler):

    def check_object_user_name_exist(self, ctxt, name):
        exist_user = objects.ObjectUserList.get_all(
            ctxt, filters={'uid': name})
        if exist_user:
            raise exception.InvalidInput(
                reason=_('object_user name %s already exists!') % name)

    def check_object_user_email_exist(self, ctxt, email):
        if not email:
            return
        exist_email = objects.ObjectUserList.get_all(
            ctxt, filters={'email': email})
        if exist_email:
            raise exception.InvalidInput(
                reason=_('object_user email %s already exists!') % email)

    def check_access_key_exist(self, ctxt, access_key):
        if not access_key:
            return
        exist_access_key = objects.ObjectAccessKeyList.get_all(
            ctxt, filters={'access_key': access_key})
        if exist_access_key:
            raise exception.InvalidInput(
                reason=_('object_user access_key %s already exists!')
                % access_key)

    def get_admin_user_info(self, ctxt):
        admin_user = objects.ObjectUserList.get_all(ctxt,
                                                    filters={"is_admin": 1})[0]
        return admin_user

    def object_user_info_get(self, ctxt):
        admin_user = self.get_admin_user_info(ctxt)
        radosgws = objects.RadosgwList.get_all(
            ctxt, filters={"status": "active"})
        if not radosgws:
            raise exception.InvalidInput(
                reason=_('radosgw not exists can not create user'))
        admin_access_key = objects.ObjectAccessKeyList.get_all(
            ctxt, filters={"obj_user_id": admin_user.id}
        )[0]
        return radosgws[0], admin_access_key

    def check_object_user_del(self, ctxt, rgw, uid):
        self.object_user_info_get(ctxt)
        bucket = rgw.rgw.get_bucket(uid=uid)
        if bucket:
            raise exception.Invalid(
                _('%s is already in use by bucket') % uid)


class ObjectUserHandler(ObjectUserMixin):

    def object_user_get(self, ctxt, object_user_id, expected_attrs=None):
        object_user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id, expected_attrs=expected_attrs)
        return object_user

    def object_user_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None, expected_attrs=None, tab=None):
        filters['is_admin'] = objects.ObjectUser.Not('1')
        users = objects.ObjectUserList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
        if tab == "metrics":
            for user in users:
                capactity = self.object_user_get_capacity(
                    ctxt, object_user_id=user['id'])
                user.metrics = capactity
        elif tab == 'io':
            for user in users:
                user.metrics = self.object_user_metrics_get(ctxt, user.id)
        return users

    def object_user_get_count(self, ctxt, filters=None):
        filters['is_admin'] = objects.ObjectUser.Not('1')
        return objects.ObjectUserList.get_count(ctxt, filters=filters)

    def object_user_create(self, ctxt, data):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.CREATE)
        user_name = data.get('uid')
        user_id = data.get('id')
        logger.debug('object_user begin crate, name:%s' % user_name)
        self.check_object_user_name_exist(ctxt, user_name)
        self.check_object_user_email_exist(ctxt, data.get('email'))
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        if len(data['keys']) != 0:
            self.check_access_key_exist(ctxt, data['keys'][0]['access_key'])
        object_user = objects.ObjectUser(
            ctxt, cluster_id=ctxt.cluster_id,
            status=s_fields.ObjectUserStatus.CREATING,
            uid=user_name, email=data.get('email'),
            display_name=user_name, suspended=0,
            op_mask=data['op_mask'],
            max_buckets=data['max_buckets'],
            description=data['description'],
            bucket_quota_max_size=data['bucket_quota_max_size'],
            bucket_quota_max_objects=data['bucket_quota_max_objects'],
            user_quota_max_size=data['user_quota_max_size'],
            user_quota_max_objects=data['user_quota_max_objects'],
            is_admin=0)
        object_user.create()
        self.task_submit(self._object_user_create, ctxt, user_name, data,
                         begin_action, object_user, radosgw,
                         admin_access_key, user_id)
        logger.info('object_user_create task has begin, user_name: %s',
                    user_name)
        return object_user

    def _object_user_create(self, ctxt, user_name, data,
                            begin_action, object_user,
                            radosgw, admin_access_key, user_id):
        if (data['user_quota_max_size'] == 0 and
                data['user_quota_max_objects'] == 0):
            user_qutoa_enabled = False
        else:
            user_qutoa_enabled = True
        if (data['bucket_quota_max_size'] == 0 and
                data['bucket_quota_max_objects'] == 0):
            bucket_qutoa_enabled = False
        else:
            bucket_qutoa_enabled = True
        if data['bucket_quota_max_size'] == 0:
            bucket_quota_max_size_kb = -1
        else:
            bucket_quota_max_size_kb = \
                data['bucket_quota_max_size'] / 1024
        if data['bucket_quota_max_objects'] == 0:
            bucket_quota_max_objects_k = -1
        else:
            bucket_quota_max_objects_k = \
                data['bucket_quota_max_objects'] / 1000
        if data['user_quota_max_size'] == 0:
            user_quota_max_size_kb = -1
        else:
            user_quota_max_size_kb = \
                data['user_quota_max_size'] / 1024
        if data['user_quota_max_objects'] == 0:
            user_quota_max_objects_k = -1
        else:
            user_quota_max_objects_k = \
                data['user_quota_max_objects'] / 1000

        try:
            node = self.get_first_mon_node(ctxt)
            client = self.agent_manager.get_client(node_id=node.id)
            if len(data['keys']) == 0:
                user_info = client.user_create_cmd(
                    ctxt, name=user_name,
                    display_name=user_name,
                    access_key=None,
                    secret_key=None,
                    email=data.get('email'),
                    max_buckets=data['max_buckets'])
            else:
                user_info = client.user_create_cmd(
                    ctxt, name=user_name,
                    display_name=user_name,
                    access_key=data['keys'][0]['access_key'],
                    secret_key=data['keys'][0]['secret_key'],
                    email=data.get('email'),
                    max_buckets=data['max_buckets'])
            rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                               secret_key=admin_access_key.secret_key,
                               server=str(radosgw.ip_address) +
                               ":"+str(radosgw.port)
                               )
            rgw.set_user_quota(
                uid=user_name,
                user_qutoa_enabled=user_qutoa_enabled,
                user_quota_max_size=bucket_quota_max_size_kb,
                user_quota_max_objects=user_quota_max_objects_k * 1000,
                bucket_qutoa_enabled=bucket_qutoa_enabled,
                bucket_quota_max_size=user_quota_max_size_kb,
                bucket_quota_max_objects=bucket_quota_max_objects_k * 1000
            )
            client.set_op_mask(ctxt, username=user_name,
                               op_mask=data['op_mask'])
            if len(data['keys']) == 0:
                access_key = {
                    'obj_user_id': object_user.id,
                    'access_key': user_info['keys'][0]['access_key'],
                    'secret_key': user_info['keys'][0]['secret_key'],
                    'type': "s3",
                    'description': data['description'],
                    'cluster_id': ctxt.cluster_id
                }
                key = objects.ObjectAccessKey(ctxt, **access_key)
                key.create()
            else:
                for i in data['keys']:
                    rgw.create_key(uid=user_name,
                                   access_key=i['access_key'],
                                   secret_key=i['secret_key'])
                for i in data['keys']:
                    access_key = {
                        'obj_user_id': object_user.id,
                        'access_key': i['access_key'],
                        'secret_key': i['secret_key'],
                        'type': "s3",
                        'description': i.get('key_description'),
                        'cluster_id': ctxt.cluster_id
                    }
                    key = objects.ObjectAccessKey(ctxt, **access_key)
                    key.create()
            status = s_fields.ObjectUserStatus.ACTIVE
            msg = _("create {} success").format(user_name)
            op_status = 'CREATE_SUCCESS'
        except Exception as e:
            logger.error("creat object_user error: %s", e)
            status = s_fields.ObjectUserStatus.ERROR
            msg = _("create {} error").format(user_name)
            op_status = 'CREATE_ERROR'
        object_user.status = status
        object_user.save()
        self.finish_action(begin_action, user_id, user_name,
                           status=status, err_msg=None)
        self.send_websocket(ctxt, object_user, op_status, msg)

    def object_user_delete(self, ctxt, object_user_id, force_delete):
        user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id, expected_attrs=['access_keys'])
        logger.debug('object_user begin delete: %s', user.uid)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        if not force_delete:
            self.check_object_user_del(ctxt, rgw, uid=user.uid)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.DELETE)
        user.status = s_fields.ObjectUserStatus.DELETING
        user.save()
        self.task_submit(self._object_user_delete, ctxt, user,
                         force_delete, rgw, begin_action)
        logger.info('object_user_delete task has begin, user_name: %s',
                    user.uid)
        return user

    def _object_user_delete(self, ctxt, user, force_delete,
                            rgw, begin_action):
        access_key_ids = []
        for access_key in user['access_keys']:
            access_key_ids.append(access_key['id'])
        user_name = user.uid
        try:
            rgw.rgw.remove_user(uid=user_name, purge_data=force_delete)
            for i in access_key_ids:
                key = objects.ObjectAccessKey.get_by_id(ctxt, str(i))
                key.destroy()
            user.destroy()
            buckets = objects.ObjectBucketList.get_all(
                ctxt, filters={'owner_id': user.id})
            for bucket in buckets:
                bucket.destroy()
            logger.info('object_user_delete success, name=%s', user_name)
            op_status = "DELETE_SUCCESS"
            msg = _("%s delete success") % user_name
            err_msg = None
            status = 'success'
        except Exception as e:
            logger.exception(
                'object_user_delete error,name=%s,reason:%s',
                user_name, str(e))
            op_status = "DELETE_ERROR"
            msg = _("%s delete error") % user_name
            err_msg = str(e)
            status = 'error'
        user.save()
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid,
                           status=status, err_msg=err_msg)

    def object_user_suspended_update(self, ctxt, object_user_id, data):
        logger.info('object_user: %s begin update supended: %s',
                    object_user_id, data['suspended'])
        user = objects.ObjectUser.get_by_id(ctxt, object_user_id)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        if data['suspended']:
            suspended = 1
            user.status = s_fields.ObjectUserStatus.SUSPENDED
            begin_action = self.begin_action(
                ctxt, resource_type=AllResourceType.OBJECT_USER,
                action=AllActionType.SET_OBJECT_USER_DISABLE)
        else:
            suspended = 0
            user.status = s_fields.ObjectUserStatus.ACTIVE
            begin_action = self.begin_action(
                ctxt, resource_type=AllResourceType.OBJECT_USER,
                action=AllActionType.SET_OBJECT_USER_ENABLE)
        try:
            rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                               secret_key=admin_access_key.secret_key,
                               server=str(radosgw.ip_address) +
                               ":" + str(radosgw.port)
                               )
            rgw.rgw.modify_user(uid=user.uid, suspended=suspended)
            user.suspended = suspended
            user.save()
            op_status = "UPDATE_SUCCESS"
            msg = _("%s update success") % user.uid
            err_msg = None
            logger.info('object_user: %s begin update supended',
                        object_user_id)
        except Exception as e:
            logger.exception(
                'object_user update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "UPDATE_ERROR"
            msg = _("%s update error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           err_msg=err_msg)
        return user

    def object_user_get_capacity(self, ctxt, object_user_id):
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        object_user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id)
        capacity = rgw.get_user_capacity(uid=object_user.uid)
        if capacity['size']['max'] < 0:
            capacity['size']['max'] = 0
        if capacity['objects']['max'] < 0:
            capacity['objects']['max'] = 0
        if capacity['buckets']['max'] < 0:
            capacity['buckets']['max'] = 0
        return capacity

    def object_user_key_create(self, ctxt, data, object_user_id):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.CREATE_KEY)
        user = objects.ObjectUser.get_by_id(ctxt, object_user_id)
        self.check_access_key_exist(ctxt, access_key=data['access_key'])
        logger.debug('object_user_key begin crate, name:%s' % user.uid)
        try:
            radosgw, admin_access_key = self.object_user_info_get(ctxt)
            rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                               secret_key=admin_access_key.secret_key,
                               server=str(radosgw.ip_address) +
                               ":" + str(radosgw.port))
            rgw.create_key(uid=user.uid, access_key=data['access_key'],
                           secret_key=data['secret_key'])
            key = objects.ObjectAccessKey(
                ctxt,
                obj_user_id=user.id,
                access_key=data['access_key'],
                secret_key=data['secret_key'],
                type="s3",
                description=data.get('description'),
                cluster_id=ctxt.cluster_id
            )
            key.create()
            op_status = "CREATE_KEY_SUCCESS"
            msg = _("%s create key success") % user.uid
            err_msg = None
        except Exception as e:
            logger.exception(
                'object_user_key update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "CREATE_KEY_ERROR"
            msg = _("%s create key error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, op_status,
                           err_msg=err_msg)
        return user

    def object_user_key_delete(self, ctxt, object_user_key_id):
        key = objects.ObjectAccessKey.get_by_id(
            ctxt, object_user_key_id)
        user = objects.ObjectUser.get_by_id(
            ctxt, key.obj_user_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.DELETE_KEY)
        logger.debug('object_user begin delete key: %s', user.uid)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        try:
            rgw.rgw.remove_key(access_key=key.access_key, uid=user.uid)
            key.destroy()
            logger.info('object_user_delete key success, name=%s', user.uid)
            op_status = "DELETE_KEY_SUCCESS"
            msg = _("%s delete key success") % user.uid
            err_msg = None
            status = 'success'
        except Exception as e:
            logger.exception(
                'object_user_delete key error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "DELETE_KEY_ERROR"
            msg = _("%s delete key error") % user.uid
            err_msg = str(e)
            status = 'error'
        self.send_websocket(ctxt, key, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           status, err_msg=err_msg)
        return key

    def object_user_key_update(self, ctxt, object_user_key_id, data):
        key = objects.ObjectAccessKey.get_by_id(
            ctxt, object_user_key_id)
        user = objects.ObjectUser.get_by_id(
            ctxt, key.obj_user_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.UPDATE_KEY)
        logger.debug('object_user begin update key: %s', user.uid)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        try:
            rgw.rgw.modify_user(uid=user.uid, access_key=key.access_key,
                                secret_key=data['secret_key'])
            key.secret_key = data['secret_key']
            key.description = data.get('description')
            key.save()
            op_status = "UPDATE_KEY_SUCCESS"
            msg = _("%s update key success") % user.uid
            err_msg = None
            logger.info('object_user: %s begin update key',
                        user.uid)
        except Exception as e:
            logger.exception(
                'object_user update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "UPDATE_KEY_ERROR"
            msg = _("%s update key error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, key, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           err_msg=err_msg)
        return key

    def object_user_email_update(self, ctxt, object_user_id, data):
        user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.UPDATE_EMAIL)
        logger.debug('object_user begin update email: %s', user.uid)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        try:
            rgw.rgw.modify_user(uid=user.uid, email=data['email'])
            user.email = data['email']
            user.save()
            op_status = "UPDATE_EMAIL_SUCCESS"
            msg = _("%s update email success") % user.uid
            err_msg = None
            logger.info('object_user: %s begin update email',
                        user.uid)
        except Exception as e:
            logger.exception(
                'object_user update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "UPDATE_EMAIL_ERROR"
            msg = _("%s update email error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           err_msg=err_msg)
        return user

    def object_user_set_op_mask(self, ctxt, object_user_id, data):
        user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.UPDATE_OP_MASK)
        logger.debug('object_user begin update op_mask: %s', user.uid)
        node = self.get_first_mon_node(ctxt)
        client = self.agent_manager.get_client(node_id=node.id)
        try:
            client.set_op_mask(ctxt, username=user.uid,
                               op_mask=data['op_mask'])
            user.op_mask = data['op_mask']
            user.save()
            op_status = "UPDATE_OP_MASK_SUCCESS"
            msg = _("%s update op_mask success") % user.uid
            err_msg = None
            logger.info('object_user: %s begin update op_mask',
                        user.uid)
        except Exception as e:
            logger.exception(
                'object_user update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "UPDATE_OP_MASK_ERROR"
            msg = _("%s update op_mask error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           err_msg=err_msg)
        return user

    def object_user_set_user_quota(self, ctxt, object_user_id, data):
        user = objects.ObjectUser.get_by_id(
            ctxt, object_user_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.OBJECT_USER,
            action=AllActionType.UPDATE_USER_QUOTA)
        logger.debug('object_user begin update user_quota: %s', user.uid)
        radosgw, admin_access_key = self.object_user_info_get(ctxt)
        rgw = RadosgwAdmin(access_key=admin_access_key.access_key,
                           secret_key=admin_access_key.secret_key,
                           server=str(radosgw.ip_address) +
                           ":" + str(radosgw.port)
                           )
        user_qutoa_enabled = True
        bucket_qutoa_enabled = True
        if data['bucket_quota_max_size'] == 0:
            bucket_quota_max_size_kb = -1
        else:
            bucket_quota_max_size_kb = \
                data['bucket_quota_max_size'] / 1024
        if data['bucket_quota_max_objects'] == 0:
            bucket_quota_max_objects_k = -1
        else:
            bucket_quota_max_objects_k = \
                data['bucket_quota_max_objects'] / 1000
        if data['user_quota_max_size'] == 0:
            user_quota_max_size_kb = -1
        else:
            user_quota_max_size_kb = \
                data['user_quota_max_size'] / 1024
        if data['user_quota_max_objects'] == 0:
            user_quota_max_objects_k = -1
        else:
            user_quota_max_objects_k = \
                data['user_quota_max_objects'] / 1000

        try:
            rgw.set_user_quota(
                uid=user.uid,
                user_qutoa_enabled=user_qutoa_enabled,
                user_quota_max_size=user_quota_max_size_kb,
                user_quota_max_objects=user_quota_max_objects_k * 1000,
                bucket_qutoa_enabled=bucket_qutoa_enabled,
                bucket_quota_max_size=bucket_quota_max_size_kb,
                bucket_quota_max_objects=bucket_quota_max_objects_k * 1000
            )
            rgw.rgw.modify_user(uid=user.uid, max_buckets=data['max_buckets'])
            user.user_quota_max_size = data['user_quota_max_size']
            user.user_quota_max_objects = data['user_quota_max_objects']
            user.bucket_quota_max_size = data['bucket_quota_max_size']
            user.bucket_quota_max_objects = data['bucket_quota_max_objects']
            user.max_buckets = data['max_buckets']
            user.save()
            op_status = "UPDATE_USER_QUOTA_SUCCESS"
            msg = _("%s update user_quota success") % user.uid
            err_msg = None
            logger.info('object_user: %s begin update user_quota',
                        user.uid)
        except Exception as e:
            logger.exception(
                'object_user update error,name=%s,reason:%s',
                user.uid, str(e))
            op_status = "UPDATE_USER_QUOTA_ERROR"
            msg = _("%s update user_quota error") % user.uid
            err_msg = str(e)
        self.send_websocket(ctxt, user, op_status, msg)
        self.finish_action(begin_action, user.id, user.uid, user,
                           err_msg=err_msg)
        return user
