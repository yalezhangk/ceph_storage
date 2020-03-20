import json
import logging

from rgwadmin import RGWAdmin
from rgwadmin.exceptions import RGWAdminException

from DSpace.exception import InvalidInput
from DSpace.exception import RadosgwAdminException
from DSpace.exception import RunCommandError
from DSpace.i18n import _
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class RadosgwAdminCMD(ToolBase):

    def _run_radosgw_admin(self, cmd):
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return stdout

    def zone_set(self, zone, file_path):
        cmd = ["radosgw-admin", "zone", "set", "--rgw-zone", zone, "<",
               file_path]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def zone_get(self, zone="default"):
        cmd = ["radosgw-admin", "zone", "get", "--rgw-zone", zone]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def realm_create(self, realm, default=False):
        cmd = ["radosgw-admin", "realm", "create", "--rgw-realm", realm]
        if default:
            cmd += ["--default"]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def zonegourp_create(self, zonegroup, realm, master=False, default=False):
        cmd = ["radosgw-admin", "zonegroup", "create", "--rgw-zonegroup",
               zonegroup, "--rgw-realm", realm]
        if master:
            cmd += ["--master"]
        if default:
            cmd += ["--default"]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def zone_create(self, zone, zonegroup, realm, master=False, default=False):
        cmd = ["radosgw-admin", "zone", "create", "--rgw-zone", zone,
               "--rgw-zonegroup", zonegroup, "--rgw-realm", realm]
        if master:
            cmd += ["--master"]
        if default:
            cmd += ["--default"]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def period_update(self, commit=True):
        cmd = ["radosgw-admin", "period", "update"]
        if commit:
            cmd += ["--commit"]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def user_create(self, name, display_name=None, access_key=None,
                    secret_key=None, email=None, max_buckets=None):
        logger.info("Create user using radosgw-admin for %s", name)
        cmd = ["radosgw-admin", "user", "create", "--uid", name]
        if display_name:
            cmd = cmd + ["--display-name", display_name]
        if access_key:
            cmd = cmd + ["--access-key", access_key]
        if secret_key:
            cmd = cmd + ["--secret-key", secret_key]
        if email:
            cmd += ["--email", email]
        if max_buckets:
            cmd += ["--max-buckets", str(max_buckets)]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def user_info(self, name):
        logger.info("Get user info using radosgw-admin for %s", name)
        cmd = ["radosgw-admin", "user", "info", "--uid", name]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    @staticmethod
    def _caps_check(caps):
        """
        Check capabilities params
        :param caps: caps string
        :return: None
        """
        available_cap = ["users", "usage", "buckets", "metadata"]
        cap_values = ["*", "read", "write", "read,write"]
        caps = caps.split(";")
        for cap in caps:
            if not cap:
                continue
            k_v = cap.split("=")
            if k_v[0] not in available_cap or k_v[1] not in cap_values:
                raise InvalidInput("Caps is not valid")

    def caps_add(self, username, caps):
        """
        Add user capabilities.
        :param caps: user caps. Available： users,usage,buckets,metadata.
            Example: "users=*;usage=read,write;buckets=read;metadata=*"
        :return: user info
        """
        logger.info("Add caps to user %s, caps: %s", username, caps)
        self._caps_check(caps)
        cmd = ["radosgw-admin", "user", "caps", "add", "--uid", username,
               "--caps", "'{}'".format(caps)]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def caps_rm(self, username, caps):
        """
        Remove user capabilities.
        :param caps: user caps. Available： users,usage,buckets,metadata.
        Example: users=*;usage=read,write;buckets=read;metadata=*
        :return: user info
        """
        logger.info("Remove caps to user %s, caps: %s", username, caps)
        self._caps_check(caps)
        cmd = ["radosgw-admin", "user", "caps", "rm", "--uid", username,
               "--caps", "'{}'".format(caps)]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def user_modify(self, username, op_mask=None):
        """
        Set op_mask for user.
        :param username:  User id of username
        :param op_mask: "read, write, delete" or "*"
        :return: user info
        """
        logger.info("Set op_mask to user %s, op_mask: %s", username, op_mask)
        cmd = ["radosgw-admin", "user", "modify", "--uid", username]
        if op_mask:
            cmd = cmd + ["--op-mask", op_mask]
        cmd_res = self._run_radosgw_admin(cmd)
        return json.loads(cmd_res)

    def set_op_mask(self, username, op_mask):
        self.user_modify(username, op_mask)

    def placement_create(self, name, index_pool, data_pool, index_type=0,
                         compression=None):
        """
        Create a placement rule
        :param name: placement key
        :param index_pool: index pool name
        :param data_pool: data and data extra pool name
        :param index_type: 0 is normal, 1 is indexless
        :param compression: zlib, snappy or zstd
        :return:
        """
        # create placement in zonegroup
        logger.info("Create placement %s: index-pool %s, data-pool %s, "
                    "index-type %s, compression %s", name, index_pool,
                    data_pool, str(index_type), compression)
        cmd = ["radosgw-admin", "zonegroup", "placement", "add",
               "--placement-id", name]
        self._run_radosgw_admin(cmd)
        cmd = ["radosgw-admin", "zone", "placement", "add", "--placement-id",
               name, "--index-pool", index_pool, "--data-pool", data_pool,
               "--data-extra-pool", data_pool, "--placement-index-type",
               str(index_type)]
        if compression:
            cmd += ["--compression", compression]
        self._run_radosgw_admin(cmd)

    def placement_remove(self, name):
        """
        Remove placement rule. Please check if related buckets is deleted.
        :param name: placement id
        :return:
        """
        logger.info("Remove placement %s", name)
        cmd = ["radosgw-admin", "zone", "placement", "rm",
               "--placement-id", name]
        self._run_radosgw_admin(cmd)
        cmd = ["radosgw-admin", "zonegroup", "placement", "rm",
               "--placement-id", name]
        self._run_radosgw_admin(cmd)

    def placement_modify(self, name, options):
        """
        Modify placement
        :param name: placement key
        :param options: {"index_type": 0, "compression": "zlib"}
        :return:
        """
        logger.info("Modify placement %s: %s", name, options)
        supported_options = ["index_type", "compression"]
        if not options:
            raise InvalidInput(_("Placement options cannot be empty!"))
        for op in options.keys():
            if op not in supported_options:
                raise InvalidInput(_("Option %s is not supported!") % op)

        cmd = ["radosgw-admin", "zone", "placement", "modify",
               "--placement-id", name]
        index_type = options.get("index_type")
        if index_type:
            cmd += ["--index-type", str(index_type)]
        compression = options.get("compression")
        if compression:
            cmd += ["--compression", compression]
        self._run_radosgw_admin(cmd)

    def placement_list(self):
        cmd = ["radosgw-admin", "zone", "placement", "list"]
        self._run_radosgw_admin(cmd)

    def placement_set_default(self, name):
        """
        Set placement to default, and need to restart rgw
        :param name: placement key
        :return:
        """
        logger.info("Set placement %s as default", name)
        cmd = ["radosgw-admin", "zonegroup", "placement", "default",
               "--placement-id", name]
        self._run_radosgw_admin(cmd)


class RadosgwAdmin(object):

    def __init__(self, access_key, secret_key, server, secure=False,
                 ca_bundle=None, verify=True):
        """
        Access to rgw admin api through rest api
        :param access_key: Access Key
        :param secret_key: Secret Key
        :param server: Server ip and port. ex. 192.168.18.6:7080
        :param secure: use ssl to connect server
        """
        self.rgw = RGWAdmin(access_key=access_key, secret_key=secret_key,
                            server=server, secure=secure, ca_bundle=ca_bundle,
                            verify=verify, timeout=5)

    def create_user(self, uid, display_name, email=None, key_type='s3',
                    access_key=None, secret_key=None, user_caps=None,
                    generate_key=True, suspended=False, max_buckets=None,
                    user_qutoa_enabled=False,
                    user_quota_max_size=None,
                    user_quota_max_objects=None,
                    bucket_qutoa_enabled=False,
                    bucket_quota_max_size=None,
                    bucket_quota_max_objects=None):
        """
        Create a user in rgw
        :param uid: uid
        :param display_name: Display name
        :param email: user email
        :param key_type:  s3 or swift
        :param access_key:  access key
        :param secret_key:  secret key
        :param user_caps: user capabilities
        :param max_buckets:
        :param suspended:
        :param op_mask:
        :return: user info
        """
        try:
            self.rgw.create_user(
                uid=uid, display_name=display_name, email=email,
                key_type=key_type, access_key=access_key,
                secret_key=secret_key, user_caps=user_caps,
                generate_key=generate_key, suspended=suspended,
                max_buckets=max_buckets
            )
            self.set_user_quota(
                uid, user_qutoa_enabled=user_qutoa_enabled,
                user_quota_max_size=user_quota_max_size,
                user_quota_max_objects=user_quota_max_objects,
                bucket_qutoa_enabled=bucket_qutoa_enabled,
                bucket_quota_max_size=bucket_quota_max_size,
                bucket_quota_max_objects=bucket_quota_max_objects
            )
            user_info = self.rgw.get_user(uid=uid)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return user_info

    def set_user_quota(self, uid,
                       user_qutoa_enabled=False,
                       user_quota_max_size=None,
                       user_quota_max_objects=None,
                       bucket_qutoa_enabled=False,
                       bucket_quota_max_size=None,
                       bucket_quota_max_objects=None, ):
        """
        Set user quota and user bucket quota
        :param uid:  user id
        :param user_qutoa_enabled:  True or False
        :param user_quota_max_size:  The units must be KB
        :param user_quota_max_objects:
        :param bucket_qutoa_enabled:
        :param bucket_quota_max_size: The units must be KB
        :param bucket_quota_max_objects:
        :return: user quota and user bucket quota
        """
        try:
            if user_qutoa_enabled:
                self.rgw.set_user_quota(uid=uid, quota_type="user",
                                        max_size_kb=user_quota_max_size,
                                        max_objects=user_quota_max_objects,
                                        enabled=True)
            if bucket_qutoa_enabled:
                self.rgw.set_user_quota(uid=uid, quota_type="bucket",
                                        max_size_kb=bucket_quota_max_size,
                                        max_objects=bucket_quota_max_objects,
                                        enabled=True)
            quota_info = self.get_user_quota(uid=uid)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return quota_info

    def get_user_quota(self, uid):
        """
        Get user quota and user bucket quota
        :param uid: user id
        :return: user quota and user bucket quota
        """
        try:
            user_quota = {
                "user_quota": self.rgw.get_user_quota(uid),
                "user_bucket_quota": self.rgw.get_user_bucket_quota(uid)
            }
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return user_quota

    def get_user_stats(self, uid):
        try:
            user_info = self.rgw.get_user(uid, stats=True)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return user_info.get("stats")

    def get_user_keys(self, uid, swift_keys=False):
        try:
            user_info = self.rgw.get_user(uid)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        keys = {
            "keys": user_info.get("keys")
        }
        if swift_keys:
            keys.update({"swift_keys": user_info.get("swift_keys")})
        return keys

    def create_key(self, uid, access_key, secret_key):
        """

        :param uid: user id
        :param access_key:
        :param secret_key:
        :return: A list of all user keys:
            [{'user': '', 'access_key': '', 'secret_key': ''}]
        """
        try:
            keys = self.rgw.create_key(uid, access_key=access_key,
                                       secret_key=secret_key)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return keys

    def modify_key(self, uid, access_key, secret_key):
        try:
            self.rgw.modify_user(uid, access_key=access_key,
                                 secret_key=secret_key)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)

    def user_suspended(self, uid):
        try:
            self.rgw.modify_user(uid, suspended=1)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)

    def user_enable(self, uid):
        try:
            self.rgw.modify_user(uid, suspended=0)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)

    def count_user_buckets(self, uid):
        try:
            bucket_list = self.rgw.get_bucket(uid=uid)
            count = len(bucket_list)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return count

    def get_user_capacity(self, uid):
        user_quota = self.get_user_quota(uid)
        try:
            user_info = self.rgw.get_user(uid)
            user_stats = self.rgw.get_user_quota(uid)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return {
            "size": {
                # check_on_raw is false, if true used is size_kb
                "used": user_stats.get("size_kb_actual"),
                "max": user_quota.get("max_size_kb")
            },
            "objects": {
                "used": user_stats.get("num_objects"),
                "max": user_quota.get("max_objects")
            },
            "buckets": {
                "used": self.count_user_buckets(uid),
                "max": user_info.get("max_buckets")
            }
        }

    def bucket_owner_change(self, bucket, bucket_id, uid):
        """
        Change bucket owner
        :param bucket:  bucket name
        :param bucket_id:  bucket id in rgw
        :param uid:  new user id
        :return:
        """
        try:
            self.rgw.link_bucket(bucket, bucket_id, uid)
            bucket_info = self.rgw.get_bucket(bucket=bucket)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return bucket_info

    def get_bucket_quota(self, bucket):
        try:
            bucket_info = self.rgw.get_bucket(bucket=bucket, )
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
        return bucket_info.get("bucket_quota")

    def bucket_remove(self, bucket, force=False):
        try:
            self.rgw.remove_bucket(bucket, purge_objects=force)
        except RGWAdminException as e:
            raise RadosgwAdminException(reason=e)
