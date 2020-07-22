import json
import time

import six
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import context as context_tool
from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSM.base import AdminBaseHandler
from DSpace.DSM.base import AdminBaseMixin
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.tools.base import SSHExecutor
from DSpace.tools.docker import Docker as DockerTool
from DSpace.utils import retry

logger = logging.getLogger(__name__)


class ServiceHelper(AdminBaseMixin):
    id = None
    last_status = None
    _last_update_at = None
    _obj = None
    STATUS_FIELDS = {
        "radosgw": s_fields.RadosgwStatus,
        "router": s_fields.RouterServiceStatus,
        "normal": s_fields.ServiceStatus,
    }

    def __init__(self, ctxt, service_obj, service_name, status, node,
                 service_type=None):
        super(ServiceHelper, self).__init__()
        self.ctxt = ctxt
        self._obj = service_obj
        self.id = service_obj.id
        self.obj_name = service_obj.name
        self.node = node
        self.service_name = service_name
        self.last_status = service_obj.status
        self.status = status
        self._last_update_at = timeutils.utcnow()
        self.service_type = service_type if service_type else "normal"
        self.status_field = self.STATUS_FIELDS[self.service_type]
        self.is_timeout = False

    def last_update_interval(self):
        return timeutils.utcnow() - self._last_update_at

    def get_status(self):
        """Get status of service"""
        pass

    def to_active(self):
        """Mark service to active"""
        res = self._obj.conditional_update({
            "status": self.status_field.ACTIVE
        }, expected_values={
            "status": [self.status_field.ACTIVE,
                       self.status_field.STARTING,
                       self.status_field.INACTIVE,
                       self.status_field.ERROR]
        })
        if res:
            msg = _("Node {}: service {} status is active"
                    ).format(self.node.hostname, self.obj_name)
            self.send_service_alert(
                self.ctxt, self._obj, "service_status", self.obj_name, "INFO",
                msg, "SERVICE_ACTIVE")

    def to_error(self):
        """Mark service to error"""
        res = self._obj.conditional_update({
            "status": s_fields.ServiceStatus.ERROR
        }, expected_values={
            "status": s_fields.ServiceStatus.STARTING
        })
        if res:
            msg = _("Node {}: service {} status is ERROR"
                    ).format(self.node.hostname, self.obj_name)
            self.send_service_alert(
                self.ctxt, self._obj, "service_status", self.obj_name, "INFO",
                msg, "SERVICE_ERROR")

    def to_inactive(self):
        """Mark service to active"""
        if self.node.status in [s_fields.NodeStatus.DEPLOYING_ROLE,
                                s_fields.NodeStatus.REMOVING_ROLE]:
            return
        res = self._obj.conditional_update({
            "status": self.status_field.INACTIVE
        }, expected_values={
            "status": self.status_field.ACTIVE
        })
        if res:
            msg = _("Node {}: service {} status is inactive"
                    ).format(self.node.hostname, self.obj_name)
            self.send_service_alert(
                self.ctxt, self._obj, "service_status", self.obj_name, "WARN",
                msg, "SERVICE_INACTIVE")

    def _check_restart(self):
        if not CONF.service_auto_restart:
            logger.info("Service auto restart is not enabled")
            return False

        auto_restart_ignore = objects.sysconfig.sys_config_get(
            self.ctxt, s_fields.ConfigKey.AUTO_RESTART_IGNORE)
        auto_restart_ignore = auto_restart_ignore.lower().split(",")
        if self.obj_name.lower() in auto_restart_ignore:
            logger.info(
                "Service %s is in auto_restart_ignore list, ignore",
                self.obj_name
            )
            return False

        # TODO: add other status
        if not self.if_service_alert(ctxt=self.ctxt, node=self.node):
            logger.info("Node %s(id %s) or cluster %s is deleting or "
                        "creating, ignore", self.node.hostname, self.node.id,
                        self.ctxt.cluster_id)
            return False
        return True

    def to_starting(self):
        """Mark service to starting"""
        if not self._check_restart():
            return
        res = self._obj.conditional_update({
            "status": self.status_field.STARTING
        }, expected_values={
            "status": self.status_field.INACTIVE
        })
        if res:
            self.task_submit(self.do_restart)

    def _do_restart(self):
        pass

    def _do_ssh_restart(self):
        pass

    def do_restart(self):
        """Restart service

        If restart failed, then go to_error.
        """

        @retry(exception.RestartServiceFailed, retries=5)
        def _restart_retry():
            if self.obj_name == "DSA":
                self._do_ssh_restart()
            else:
                self._do_restart()

        msg = _("Node {}：service {} status is down, trying to restart") \
            .format(self.node.hostname, self.obj_name)
        logger.warning(msg)
        self.send_websocket(self.ctxt, self._obj, "SERVICE_RESTART", msg)
        try:
            _restart_retry()
        except exception.StorException as e:
            logger.warning(e)
            self.to_error()

    def get_context(self):
        return self.ctxt

    def get_object(self):
        return self._obj

    def get_status_field(self):
        return self.STATUS_FIELDS[self.service_type]


class SystemdHelper(ServiceHelper):

    def __init__(self, *args, **kwargs):
        super(SystemdHelper, self).__init__(*args, **kwargs)

    def get_status(self):
        """Get status of service"""
        return self._obj.status

    def _do_restart(self):
        client = context_tool.agent_manager.get_client(node_id=self.node.id)
        try:
            client.systemd_service_restart(self.ctxt, self.service_name)
            if (client.systemd_service_status(self.ctxt, self.service_name) ==
                    "active"):
                logger.info("%s on node %s(id %s) has been restarted",
                            self.obj_name, self.node.hostname, self.node.id)
                return
        except exception.StorException as e:
            logger.warning("Restart %s on node %s(id %s) failed: %s",
                           self.obj_name, self.node.hostname, self.node.id, e)
        raise exception.RestartServiceFailed(service=self.obj_name)


class ContainerHelper(ServiceHelper):

    def __init__(self, *args, **kwargs):
        super(ContainerHelper, self).__init__(*args, **kwargs)

    def get_status(self):
        """Get status of service"""
        return self._obj.status

    def _do_restart(self):
        client = context_tool.agent_manager.get_client(node_id=self.node.id)
        try:
            client.docker_service_restart(self.ctxt, self.service_name)
            if (client.docker_servcie_status(self.ctxt, self.service_name) ==
                    "running"):
                logger.info("%s on node %s(id %s) has been restarted",
                            self.obj_name, self.node.hostname, self.node.id)
                return
        except exception.StorException as e:
            logger.warning("Restart %s on node %s(id %s) failed: %s",
                           self.obj_name, self.node.hostname, self.node.id, e)
        raise exception.RestartServiceFailed(service=self.obj_name)

    def _do_ssh_restart(self):
        ssh = SSHExecutor(hostname=str(self.node.ip_address),
                          password=self.node.password)
        docker_tool = DockerTool(ssh)
        try:
            docker_tool.restart(self.service_name)
            if docker_tool.status(self.service_name):
                logger.info("%s on node %s(id %s) has been restarted",
                            self.obj_name, self.node.hostname, self.node.id)
                return
        except exception.StorException as e:
            logger.warning("Restart %s on node %s(id %s) failed: %s",
                           self.obj_name, self.node.hostname, self.node.id, e)
        raise exception.RestartServiceFailed(service=self.obj_name)


class ServiceManager(AdminBaseMixin):
    _services = None

    def __init__(self):
        super(ServiceManager, self).__init__()
        self.ctxt = context_tool.get_context()
        self._services = {}
        self.add_dsa_service_helper()

    def add_dsa_service_helper(self):
        dsas = objects.ServiceList.get_all(
            self.ctxt, filters={'name': 'DSA', 'cluster_id': '*'})
        for dsa in dsas:
            ctxt = context_tool.get_context(cluster_id=dsa.cluster_id)
            node = objects.Node.get_by_id(ctxt, dsa.node_id)
            dsa_helper = ContainerHelper(
                ctxt, dsa, self.container_prefix + "_dsa", dsa.status, node)
            self.append("base", dsa_helper)

    def loop(self):
        while True:
            for role in list(self._services.keys()):
                service_list = self._services[role]
                for service_id in list(service_list.keys()):
                    try:
                        self._check(role, service_list[service_id])
                    except exception.StorException as e:
                        logger.warning("Check status error: %s", e)
            time.sleep(CONF.service_heartbeat_interval)

    def _check_time_interval(self, helper):
        if helper.is_timeout:
            return False
        if (helper.last_update_interval().total_seconds() >
                CONF.service_max_interval):
            logger.warning("Service %s on node %s(id %s) is timeout, "
                           "mark it to inactive", helper.obj_name,
                           helper.node.hostname, helper.node.id)
            helper.status = helper.status_field.INACTIVE
            helper.is_timeout = True
            if helper.obj_name != "DSA":
                return False
        return True

    def _check_node(self, role, helper):
        # Remove helper if node is deleted
        try:
            helper.node = objects.Node.get_by_id(helper.ctxt, helper.node.id)
        except exception.StorException as e:
            logger.warning(e)
            self.remove(role, helper.id)
            return False
        # check node status
        if helper.node.status in [s_fields.NodeStatus.DELETING]:
            logger.warning("Node %s(id %s) status is %s, "
                           "ignore service update",
                           helper.node.hostname, helper.node.id,
                           helper.node.status)
            return False
        if not self.if_service_alert(helper.ctxt):
            logger.warning("Cluster %s is deleting, ignore service update",
                           helper.ctxt.cluster_id)
            return False
        return True

    def _check(self, role, helper):
        logger.debug("Check service status, name: %s, id: %s, status: %s, "
                     "last_status: %s", helper.obj_name, helper.id,
                     helper.status, helper.last_status)
        if (not self._check_node(role, helper) or
                not self._check_time_interval(helper)):
            return
        status = helper.status
        last_status = helper.last_status
        service = helper.get_object()
        status_field = helper.get_status_field()
        if status not in [
            status_field.ACTIVE,
            status_field.INACTIVE,
            status_field.STARTING,
            status_field.ERROR
        ]:
            return
        if (last_status in [status_field.STARTING, status_field.ERROR] and
                status != status_field.ACTIVE):
            return
        if status == status_field.INACTIVE:
            if helper.node.status == s_fields.NodeStatus.DELETING:
                return
            if helper.obj_name == "DSA" or last_status == status_field.ACTIVE:
                helper.to_inactive()
                helper.to_starting()
        if status == status_field.ACTIVE:
            service.counter += 1
            try:
                service.save()
            except exception.NotFound as e:
                logger.warning(e)
                self.remove(role, service.id)
                return
            if last_status in [status_field.INACTIVE,
                               status_field.STARTING,
                               status_field.ERROR]:
                if helper.node.status == s_fields.NodeStatus.DELETING:
                    return
                helper.to_active()

    def append(self, role, service_helper):
        if not self._services.get(role):
            self._services[role] = {}
        self._services[role][service_helper.id] = service_helper
        logger.debug("Add to service list, role: %s, name: %s, service id: "
                     "%s, status: %s", role, service_helper.service_name,
                     service_helper.id, service_helper.status)

    def remove(self, role, service_id):
        self._services[role].pop(service_id)


class ServiceHandler(AdminBaseHandler):
    def __init__(self):
        super(ServiceHandler, self).__init__()
        self.container_roles = self.map_util.container_roles
        self.service_manager = ServiceManager()

    def bootstrap(self):
        super(ServiceHandler, self).bootstrap()
        if CONF.heartbeat_check:
            self.task_submit(self._service_check, permanent=True)

    def _service_check(self):
        self.wait_ready()
        self.service_manager.loop()

    def _check_service_status(self, ctxt, services):
        for service in services:
            self.check_service_status(ctxt, service)

    def services_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                         sort_dirs=None, filters=None, offset=None):
        services = objects.ServiceList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        self._check_service_status(ctxt, services)
        return services

    def service_get_count(self, ctxt, filters=None):
        return objects.ServiceList.get_count(ctxt, filters=filters)

    def get_service_obj(self, ctxt, filters, name, status, node, role):
        service = objects.ServiceList.get_all(ctxt, filters=filters)
        if not service:
            if role in ["base", "role_monitor", "role_block_gateway"]:
                return None
            service_db = objects.Service(
                ctxt, name=name, status=status,
                node_id=node.id, cluster_id=ctxt.cluster_id,
                counter=0, role=role
            )
            service_db.create()
            return service_db
        else:
            service_db = service[0]
        return service_db

    def service_update(self, ctxt, services, node_id):
        logger.info("service update from node(%s): %s", node_id, services)
        node = objects.Node.get_by_id(ctxt, node_id)
        services = json.loads(services)
        logger.info('Update service status for node %s(id %s)',
                    node.hostname, node_id)
        for role, sers in six.iteritems(services):
            for s in sers:
                name = s.get('name')
                filters = {
                    "name": name,
                    "node_id": s['node_id']
                }
                status = s.get('status')
                service_name = s.get('service_name')
                if role == "role_object_gateway":
                    rgw = objects.RadosgwList.get_all(ctxt, filters=filters)
                    if not rgw:
                        continue
                    service_helper = SystemdHelper(
                        ctxt, rgw[0], service_name, status, node,
                        service_type="radosgw"
                    )
                elif role == "role_radosgw_router":
                    filters["name"] = filters.get("name").split("_")[1]
                    router_service = objects.RouterServiceList.get_all(
                        ctxt, filters=filters)
                    if not router_service:
                        continue
                    service_helper = ContainerHelper(
                        ctxt, router_service[0], service_name, status, node,
                        service_type="router")
                else:
                    service_obj = self.get_service_obj(
                        ctxt, filters, name, status, node, role)
                    if not service_obj:
                        continue
                    if role in self.container_roles:
                        service_helper = ContainerHelper(
                            ctxt, service_obj, service_name, status, node)
                    else:
                        service_helper = SystemdHelper(
                            ctxt, service_obj, service_name, status, node)
                self.service_manager.append(role, service_helper)
        return True

    def service_infos_get(self, ctxt, node):
        logger.info("Get uncertain service for node %s(id %s)",
                    node.hostname, node.id)
        services = {}
        filters = {'node_id': node.id}
        if node.role_object_gateway:
            # get rgw service
            radosgws = objects.RadosgwList.get_all(ctxt, filters=filters)
            if radosgws:
                services.update({'radosgws': radosgws})
            # get rgw router service
            rgw_routers = objects.RouterServiceList.get_all(
                ctxt, filters=filters)
            if rgw_routers:
                services.update({'radosgw_routers': rgw_routers})
        if node.role_file_gateway:
            # TODO return file gateway
            pass
        return services
