#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import six
from oslo_versionedobjects import exception as obj_exc

from DSpace.i18n import _

logger = logging.getLogger(__name__)


class StorException(Exception):
    """Base Stor Exception

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.

    """
    message = _("An unknown exception occurred.")
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if message:
            self.message = message

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        for k, v in self.kwargs.items():
            if isinstance(v, Exception):
                self.kwargs[k] = six.text_type(v)

        try:
            message = self.message % kwargs
        except Exception:
            # NOTE(melwitt): This is done in a separate method so it can be
            # monkey-patched during testing to make it a hard failure.
            self._log_exception()
            message = self.message

        self.msg = message
        self.code = self.kwargs['code']
        super(StorException, self).__init__(message)

    def _log_exception(self):
        # kwargs doesn't match a variable in the message
        # log the issue and the kwargs
        logger.exception('Exception in string format operation:')
        logger.error(self.__class__.__name__)
        for name, value in self.kwargs.items():
            logger.error("%(name)s: %(value)s",
                         {'name': name, 'value': value})

    def __unicode__(self):
        return self.msg

    @classmethod
    def obj_cls(cls):
        return cls.__name__


ObjectActionError = obj_exc.ObjectActionError


class ProgrammingError(StorException):
    message = _('Programming error in Stor: %(reason)s')


class NotAuthorized(StorException):
    message = _("Not authorized.")
    code = 401


class PasswordError(NotAuthorized):
    message = _("User or password was error.")


class NotFound(StorException):
    message = _("Resource could not be found.")
    code = 404
    safe = True


class Duplicate(StorException):
    code = 409
    safe = True


class UserNotFound(NotFound):
    message = _("User %(user_id)s could not be found.")


class TaskflowNotFound(NotFound):
    message = _("Taskflow %(taskflow_id)s could not be found.")


class TaskflowArgsError(StorException):
    message = _("Taskflow %(taskflow_id)s args %(args) error.")


class TaskNotFound(NotFound):
    message = _("Task %(task_id)s could not be found.")


class SysConfigNotFound(NotFound):
    message = _("Sys Config %(sys_config_id)s could not be found.")


class VolumeNotFound(NotFound):
    message = _("Volume %(volume_id)s could not be found.")


class ClusterNotFound(NotFound):
    message = _("Cluster %(cluster_id)s could not be found.")


class RPCServiceNotFound(NotFound):
    message = _("RPCService %(rpc_service_id)s could not be found.")


class RPCConnectError(StorException):
    code = 400
    message = _("RPC service connect error")


class NodeNotFound(NotFound):
    message = _("Node %(node_id)s could not be found.")


class NodeRolesUpdateError(StorException):
    code = 400
    message = _("Node %(node)s roles update error.")


class SystemctlRestartError(StorException):
    code = 200
    message = _('Service %(service)s restart failed. State: %(state)s')


class NodeMoveNotAllow(StorException):
    code = 400
    message = _("node: %(node)s osd already in pool %(pool)s, can't move")


class RackMoveNotAllow(StorException):
    code = 400
    message = _("rack: %(rack)s osd already in a pool, can't move")


class DatacenterNotFound(NotFound):
    message = _("Datacenter %(datacenter_id)s could not be found.")


class RackNotFound(NotFound):
    message = _("Rack %(rack_id)s could not be found.")


class OsdNotFound(NotFound):
    message = _("Osd %(osd_id)s could not be found.")


class PoolNotFound(NotFound):
    message = _("Pool %(pool_id)s could not be found.")


class NetworkNotFound(NotFound):
    message = _("Network %(net_id)s could not be found.")


class DiskNotFound(NotFound):
    message = _("Disk %(disk_id)s could not be found.")


class RadosgwNotFound(NotFound):
    message = _("Radosgw %(radosgw_id)s could not be found.")


class RgwZoneNotFound(NotFound):
    message = _("Radosgw Zone %(rgw_zone_id)s could not be found.")


class RgwRouterNotFound(NotFound):
    message = _("Radosgw router %(rgw_router_id)s could not be found.")


class DiskPartitionNotFound(NotFound):
    message = _("Disk partition %(disk_part_id)s could not be found.")


class ServiceNotFound(NotFound):
    message = _("Service %(service_id)s could not be found.")


class ObjectPolicyNotFound(NotFound):
    message = _("ObjectPolicy %(object_policy_id)s could not be found.")


class ClusterIDNotFound(NotFound):
    code = 400
    message = _("Cluster ID could not be found.")


class EndpointNotFound(NotFound):
    code = 400
    message = _("Endpoint for %(service_name)s:%(node_id)s "
                "could not be found.")


class PrometheusEndpointNotFound(NotFound):
    code = 400
    message = _("Endpoint for %(service_name)s:%(cluster_id)s "
                "could not be found.")


class ClusterExists(Duplicate):
    message = _("Cluster %(cluster_id)s already exists.")


class NoSuchMethod(NotFound):
    message = _("Method %(method)s not found.")


class RunCommandError(StorException):
    message = _("Command: %(cmd)s ReturnCode: %(return_code)s "
                "Stderr: %(stderr)s Stdout: %(stdout)s.")


class RunCommandArgsError(StorException):
    message = _("Argument 'args' to run_command must be list or string")


class NodeLowSpaceException(StorException):
    message = _("Node sys disk space less than %(percent)s%%")


class CephException(StorException):
    message = _("Ceph exception")


class CephConnectTimeout(StorException):
    message = _("error connecting to the cluster")


class CephCommandTimeout(StorException):
    message = _("Ceph command timeout")


class DataBalanceActionError(CephException):
    message = _("Data balance %(action)s error, mode %(mode)s")


class Invalid(StorException):
    message = _("%(msg)s")
    code = 400


class SSHException(StorException):
    message = _("%(msg)s")
    code = 400


class SSHAuthInvalid(SSHException):
    message = _("SSH Error: ip(%(ip)s) Authentication failed")
    code = 400


class SSHPasswordRequiredException(SSHException):
    message = _("SSH Error: ip(%(ip)s) Need provide password")
    code = 400


class SSHBadAuthenticationType(SSHException):
    message = _("SSH Error: ip(%(ip)s) Unsupported authentication types")
    code = 400


class SSHPartialAuthentication(SSHException):
    message = _("SSH Error: ip(%(ip)s) Internal authentication exception")
    code = 400


class SSHChannelException(SSHException):
    message = _("SSH Error: ip(%(ip)s) Open a new channel exception")
    code = 400


class SSHBadHostKeyException(SSHException):
    message = _("SSH Error: ip(%(ip)s) Host keys do not match")
    code = 400


class SSHProxyCommandFailure(SSHException):
    message = _("SSH Error: ip(%(ip)s) Check the SSH configuration file")
    code = 400


class SSHConnectException(SSHException):
    message = _("SSH Error: ip(%(ip)s) Can not connect")
    code = 400


class VolumeAccessPathNotFound(NotFound):
    message = _("VolumeAccessPath %(access_path_id)s could not be found.")


class VolumeGatewayNotFound(NotFound):
    message = _("VolumeGateway %(gateway_id)s could not be found.")


class VolumeClientNotFound(NotFound):
    message = _("VolumeClient %(volume_client_id)s could not be found.")


class VolumeClientGroupNotFound(NotFound):
    message = _("VolumeClientGroup %(client_group_id)s could not be found.")


class VolumeClientExists(Duplicate):
    message = _("client %(name)s already exists.")


class VolumeClientGroupExists(Duplicate):
    message = _("VolumeClientGroup %(volume_client_group_name)s "
                "already exists.")


class VolumeClientGroupNoMapping(NotFound):
    message = _("no mapping attached to this client group: "
                "%(volume_client_group)s")


class VolumeClientGroupEnableChapError(Invalid):
    message = _("access path %(access_path)s don't enable chap, "
                "can't enable mutual chap for client group "
                "%(volume_client_group)s")


class VolumeClientGroupDeleteError(StorException):
    message = _("can't delete client group: %(reason)s")


class LicenseNotFound(NotFound):
    message = _("License %(license_id)s could not be found.")


class InvalidInput(Invalid):
    message = _("%(reason)s")


class AlertRuleNotFound(NotFound):
    message = _("AlertRule %(alert_rule_id)s could not be found.")


class EmailGroupNotFound(NotFound):
    message = _("EmailGroup %(email_group_id)s could not be found.")


class CephConfigNotFound(NotFound):
    message = _("CephConfig %(ceph_config_id)s could not be found.")


class CephConfigKeyNotFound(NotFound):
    message = _("CephConfig %(group)s: %(key)s could not be found.")


class AlertGroupNotFound(NotFound):
    message = _("AlertGroup %(alert_group_id)s could not be found.")


class EmailGroupDeleteError(StorException):
    message = _("can not delete email_group used by alert_group")
    code = 400


class AlertLogNotFound(NotFound):
    message = _("AlertLog %(alert_log_id)s could not be found.")


class LogFileNotFound(NotFound):
    message = _("LogFile %(log_file_id)s could not be found.")


class LedNotSupport(StorException):
    code = 400
    message = _("Disk %(disk_id)s do not support led light")


class VolumeSnapshotNotFound(NotFound):
    message = _("VolumeSnapshot %(volume_snapshot_id)s could not be found.")


###############################

class VolumeActionNotFound(NotFound):
    message = _("Volume Action %(action)s must in"
                "'extend,shrink,rollback,unlink'")


class ObjectBucketActionNotFound(NotFound):
    message = _("ObjectBucket Action %(action)s must in"
                "'quota, owner'")


class DiskActionNotFound(NotFound):
    message = _("Disk Action %(action)s do not support")


class CrushRuleNotFound(NotFound):
    message = _("CrushRule %(crush_rule_id)s could not be found.")


class VolumeSnapshotActionNotFound(NotFound):
    message = _("VolumeSnapshot Action %(action)s must in 'clone'")


class PoolExists(Duplicate):
    message = _("a pool named %(pool)s is exists")


class PoolNameNotFound(StorException):
    message = _("a pool named %(pool)s not found")


class VolumeStatusNotAllowAction(StorException):
    code = 400
    message = _('volume status must in [active, error]')


class ActionLogNotFound(NotFound):
    message = _("ActionLog %(action_log_id)s could not be found.")


class AccessPathExists(Duplicate):
    message = _("%(access_path)s could not be found.")


class IscsiTargetError(StorException):
    message = _('iscsi target operation error: %(action)s')


class IscsiTargetExists(Duplicate):
    message = _("iscsi target %(iqn)s already exists.")


class IscsiTargetNotFound(NotFound):
    message = _("iscsi target %(iqn)s not found.")


class IscsiAclExists(Duplicate):
    message = _("iscsi acl %(iqn)s already exists.")


class IscsiBackstoreExists(Duplicate):
    message = _("iscsi backstore %(disk_name)s already exists.")


class IscsiBackstoreNotFound(NotFound):
    message = _("iscsi backstore %(disk_name)s not found.")


class IscsiAclNotFound(NotFound):
    message = _("iscsi acl %(iqn_initiator)s not found.")


class IscsiAclMappedLunNotFound(NotFound):
    message = _("iscsi acl %(iqn_initiator)s can't find a mapped lun "
                "attached to %(disk_name)s")


class AlertLogActionNotFound(NotFound):
    message = _("AlertLog Action %(action)s must in"
                "'all_readed,del_alert_logs'")


class VolumeGatewayExists(Duplicate):
    message = _("a volume gateway already mount in node %(node)s.")


class AccessPathVolumeExists(Duplicate):
    message = _("access path already has volume %(volume)s.")


class AccessPathNoGateway(NotFound):
    message = _("access path %(access_path)s no volume gateway.")


class AccessPathNoMapping(NotFound):
    message = _("access path %(access_path)s no such mapping.")


class AccessPathUnmountBgwError(StorException):
    message = _("can't unmount bgw: %(reason)s")


class AccessPathNoVolmues(NotFound):
    message = _("access path %(access_path)s no more volumes, can't remove.")


class AccessPathNoSuchVolume(NotFound):
    message = _("access path %(access_path)s no such volume: %(volume)s.")


class VolumeMappingNotFound(NotFound):
    message = _("volume mapping %(volume_mapping_id)s not exists.")


class AccessPathMappingVolumeExists(Duplicate):
    message = _("access path mapping %(access_path)s:%(client_group)s "
                "already has volume %(volume)s.")


class AccessPathDeleteError(StorException):
    message = _("can't delete access path: %(reason)s")


class DownloadFileError(StorException):
    message = _('download file error: %(reason)s')


class GetFileSizeError(StorException):
    message = _('get file size error: %(reason)s')


class ActionTimeoutError(StorException):
    code = 400
    message = _('Timeout error: %(reason)s')


class ClusterPauseError(StorException):
    message = _('Osd pause error')


class ClusterUnpauseError(StorException):
    message = _('Osd unpause error')


class ClusterNotHealth(StorException):
    code = 400
    message = _('Cluster not health, Please check cluster status')


class ClusterNoMonitorRole(StorException):
    code = 400
    message = _('Please set monitor role first')


class IPConnectError(StorException):
    message = _('Connect to %(ip)s error')


class LockCreationFailed(StorException):
    message = _("%(msg)s")


class OsdStatusNotUp(StorException):
    message = _("Osd status not up")


class DeviceOrResourceBusy(StorException):
    message = _("Device or resource busy")


class PermissionDenied(StorException):
    message = _("Permission denied")


class DockerSockNotFound(NotFound):
    message = _("Docker socket file %(path)s not found")


class DockerSockCmdError(StorException):
    message = _("Docker socket cmd %(cmd)s error: %(reason)s")


class DbusSocketNotFound(NotFound):
    message = _("Dbus socket file %(path)s not found")


class RunDbusError(StorException):
    message = _("Dbus command for %(service)s error: %(reason)s")


class RestartServiceFailed(StorException):
    message = _("Restart service %(service)s failed")


class UserorPasswordError(StorException):
    message = _("User or Password error")


class RadosgwAdminException(StorException):
    message = _("Radosgw Amdin operation error: %(reason)s")


class S3ClientError(StorException):
    message = _("S3 Client Error")


class CompressionAlgorithmNotFound(NotFound):
    message = _('compression_algorithm: %(compression)s not found')


class ObjectPolicyActionNotFound(NotFound):
    message = _("ObjectPolicy Action %(action)s must in "
                "'default, compression'")


class ObjectUserNotFound(NotFound):
    message = _("ObjectUser %(object_user_id)s could not be found.")


class ObjectUserNotActive(StorException):
    message = _("Object user %(user)s is not active. "
                "Current status is %(status)s")


class OpMaskError(StorException):
    message = _("Current operation requires '%(op_type)s' on user")


class ObjectAccessKeyNotFound(NotFound):
    message = _("ObjectAccessKey %(object_access_key_id)s could not be found.")


class ObjectBucketNotFound(NotFound):
    message = _("ObjectBucket %(object_bucket_id)s could not be found.")


class ObjectLifecycleNotFound(NotFound):
    message = _("ObjectLifecycle %(object_lifecycle_id)s could not be found.")


class LogoNotFound(NotFound):
    message = _("Logo %(logo_name)s could not be found.")


class CrushMapNotFound(NotFound):
    message = _("CrushMap is not found in %(path)s.")
