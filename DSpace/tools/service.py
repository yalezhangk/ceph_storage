#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

from DSpace.exception import DbusSocketNotFound
from DSpace.exception import RunCommandError
from DSpace.exception import RunDbusError
from DSpace.tools.base import ToolBase

try:
    import dbus
except ImportError:
    dbus = None

logger = logging.getLogger(__name__)


class Service(ToolBase):
    def enable(self, name):
        logger.debug("Service enable: {}".format(name))
        cmd = ["systemctl", "enable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def start(self, name):
        logger.debug("Service start: {}".format(name))
        cmd = ["systemctl", "start", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def stop(self, name):
        logger.debug("Service stop: {}".format(name))
        cmd = ["systemctl", "stop", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        if "not loaded" in stderr:
            return False
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def disable(self, name):
        logger.debug("Service disable: {}".format(name))
        cmd = ["systemctl", "disable", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        if "No such file" in stderr:
            return False
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def restart(self, name):
        logger.debug("Service restart: {}".format(name))
        cmd = ["systemctl", "restart", name]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def status(self, name):
        logger.debug("Check service status: {}".format(name))
        cmd = ["systemctl", "status", name, "|", "grep", "Active", "|",
               "awk", "'{{print $2}}'"]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        status = stdout.strip()
        if status != "active":
            return False
        return True


class ServiceDbus(ToolBase):
    DBUS_FILE = "system_bus_socket"
    DBUS_PATH = "/run/dbus/"

    def __init__(self, *args, **kwargs):
        super(ServiceDbus, self).__init__(*args, **kwargs)
        self._check_dbus_file()

    def _check_dbus_file(self):
        link_file = os.path.join(self.DBUS_PATH, self.DBUS_FILE)
        dbus_file = self._wapper(link_file)
        if os.path.exists(link_file):
            return True
        else:
            if os.path.exists(dbus_file):
                os.mkdir(self.DBUS_PATH)
                os.symlink(dbus_file, link_file)
                return True
        raise DbusSocketNotFound(path=link_file)

    def status(self, service):
        try:
            bus = dbus.SystemBus()
            systemd = bus.get_object('org.freedesktop.systemd1',
                                     '/org/freedesktop/systemd1')
            manager = dbus.Interface(
                systemd, dbus_interface='org.freedesktop.systemd1.Manager')
            try:
                unit = manager.GetUnit(service)
            except dbus.exceptions.DBusException as e:
                if "not loaded" in str(e):
                    return "inactive"
                raise e

            unit_proxy = bus.get_object('org.freedesktop.systemd1', str(unit))
            unit_properties = dbus.Interface(
                unit_proxy, dbus_interface='org.freedesktop.DBus.Properties')
            res = unit_properties.Get('org.freedesktop.systemd1.Unit',
                                      'ActiveState')
            return str(res)
        except dbus.exceptions.DBusException as e:
            logger.warning("Get service status error: %s", e)
            raise RunDbusError(service=service, reason=str(e))

    def restart(self, service):
        try:
            bus = dbus.SystemBus()
            systemd = bus.get_object('org.freedesktop.systemd1',
                                     '/org/freedesktop/systemd1')
            manager = dbus.Interface(systemd,
                                     'org.freedesktop.systemd1.Manager')
            manager.RestartUnit(service, 'fail')
        except dbus.exceptions.DBusException as e:
            logger.warning("Restart service error: %s", e)
            raise RunDbusError(service=service, reason=str(e))

    def reset_failed(self, service):
        try:
            bus = dbus.SystemBus()
            systemd = bus.get_object('org.freedesktop.systemd1',
                                     '/org/freedesktop/systemd1')
            manager = dbus.Interface(systemd,
                                     'org.freedesktop.systemd1.Manager')
            manager.ResetFailedUnit(service)
        except dbus.exceptions.DBusException as e:
            logger.warning("Reset-failed service error: %s", e)
            raise RunDbusError(service=service, reason=str(e))
