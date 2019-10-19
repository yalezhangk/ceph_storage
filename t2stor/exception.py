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
    pass


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


class DatacenterNotFound(NotFound):
    message = _("Datacenter %(datacenter_id)s could not be found.")


class RackNotFound(NotFound):
    message = _("Rack %(rack_id)s could not be found.")


class OsdNotFound(NotFound):
    message = _("Osd %(osd_id)s could not be found.")


class ClusterIDNotFound(NotFound):
    code = 400
    message = _("Cluster ID could not be found.")


class EndpointNotFound(NotFound):
    code = 400
    message = _("Endpoint for %(service_name)s:%(hostname)s "
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


class VolumeAccessPathNotFound(NotFound):
    message = _("VolumeAccessPath %(access_path_id)s could not be found.")


class VolumeGatewayNotFound(NotFound):
    message = _("VolumeGateway %(gateway_id)s could not be found.")


class VolumeClientNotFound(NotFound):
    message = _("VolumeClient %(volume_client_id)s could not be found.")


class VolumeClientGroupNotFound(NotFound):
    message = _("VolumeClientGroup %(client_group_id)s could not be found.")
