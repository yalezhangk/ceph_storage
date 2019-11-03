#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import six
from oslo_versionedobjects import exception as obj_exc

from t2stor.i18n import _

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
    code = 403


class NotFound(StorException):
    message = _("Resource could not be found.")
    code = 404
    safe = True


class Duplicate(StorException):
    code = 409
    safe = True


class SysConfigNotFound(NotFound):
    message = _("Sys Config %(sys_config_id)s could not be found.")


class VolumeNotFound(NotFound):
    message = _("Volume %(volume_id)s could not be found.")


class ClusterNotFound(NotFound):
    message = _("Cluster %(cluster_id)s could not be found.")


class RPCServiceNotFound(NotFound):
    message = _("RPCService %(rpc_service_id)s could not be found.")


class NodeNotFound(NotFound):
    message = _("Node %(node_id)s could not be found.")


class NodeStatusNotAllow(StorException):
    code = 400
    message = _('node status must in [active, error]')


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


class DiskPartitionNotFound(NotFound):
    message = _("Disk %(disk_part_id)s could not be found.")


class ServiceNotFound(NotFound):
    message = _("Service %(service_id)s could not be found.")


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


class CephException(StorException):
    message = _("Ceph exception")


class Invalid(StorException):
    message = _("%(msg)s")
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
    message = _("iqn %(iqn)s already exists.")


class VolumeClientGroupExists(Duplicate):
    message = _("VolumeClientGroup %(volume_client_group_name)s "
                "already exists.")


class LicenseNotFound(NotFound):
    message = _("License %(license_id)s could not be found.")


class InvalidInput(Invalid):
    message = _("Invalid input received: %(reason)s")


class AlertRuleNotFound(NotFound):
    message = _("AlertRule %(alert_rule_id)s could not be found.")


class EmailGroupNotFound(NotFound):
    message = _("EmailGroup %(email_group_id)s could not be found.")


class CephConfigNotFound(NotFound):
    message = _("CephConfig %(ceph_config_id)s could not be found.")


class CephConfigKeyNotFound(NotFound):
    message = _("CephConfig %(group):s%(key)s could not be found.")


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


class DiskActionNotFound(NotFound):
    message = _("Disk Action %(action)s do not support")


class CrushRuleNotFound(NotFound):
    message = _("CrushRule %(crush_rule_id)s could not be found.")


class VolumeSnapshotActionNotFound(NotFound):
    message = _("VolumeSnapshot Action %(action)s must in 'clone'")


class PoolExists(StorException):
    message = _("a pool named %(pool)s is exists")


class PoolNameNotFound(StorException):
    message = _("a pool named %(pool)s not found")


class VolumeStatusNotAllowAction(StorException):
    code = 400
    message = _('volume status must in [active, error]')


class ActionLogNotFound(NotFound):
    message = _("ActionLog %(action_log_id)s could not be found.")


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
