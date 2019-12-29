import json

import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool
from DSpace.tools.docker import Docker as DockerTool

logger = logging.getLogger(__name__)


class ServiceHandler(AdminBaseHandler):
    def __init__(self, *args, **kwargs):
        super(ServiceHandler, self).__init__(*args, **kwargs)
        self.container_roles = self.map_util.container_roles

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

    def _restart_systemd_service(self, ctxt, name, service, node):
        if self.debug_mode or not self.if_service_alert(ctxt, node=node):
            return
        logger.info("Try to restart systemd service: %s", name)
        service.status = s_fields.ServiceStatus.STARTING
        service.save()
        if name == "MON":
            target = node.hostname
            type = "mon"
        elif name == "MGR":
            target = node.hostname
            type = "mgr"
        elif name == "MDS":
            target = node.hostname
            type = "mds"
        else:
            logger.error("Service not support")
            return
        msg = _("Node {}: service {} in status is inactive, trying to restart"
                ).format(node.hostname, name)
        self.send_websocket(ctxt, service, "SERVICE_RESTART", msg)
        service.status = s_fields.ServiceStatus.STARTING
        service.save()
        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        ceph_tool = CephTool(ssh)
        try:
            ceph_tool.systemctl_restart(type, target)
        except exception.StorException as e:
            logger.error(e)
            service.status = s_fields.ServiceStatus.ERROR
            service.save()
            msg = _("Node {}: service {} restart failed, mark it to ERROR"
                    ).format(node.hostname, name)
            self.send_service_alert(
                ctxt, service, "service_status", "Service", "ERROR", msg,
                "SERVICE_ERROR"
            )

    def _restart_radosgw_service(self, ctxt, radosgw, node):
        if self.debug_mode or not self.if_service_alert(ctxt, node=node):
            return
        logger.info("Try to restart rgw service: %s", radosgw.name)
        radosgw.status = s_fields.RadosgwStatus.STARTING
        radosgw.save()
        target = radosgw.name
        type = "rgw"
        msg = _("Node {}: radosgw {} status is inactive, trying to restart"
                ).format(node.hostname, radosgw.display_name)
        self.send_websocket(ctxt, radosgw, "SERVICE_RESTART", msg)

        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        ceph_tool = CephTool(ssh)
        try:
            ceph_tool.systemctl_restart(type, target)
        except exception.StorException as e:
            logger.error(e)
            radosgw.status = s_fields.RadosgwStatus.ERROR
            radosgw.save()
            msg = _("Node {}: radosgw {} restart failed, mark it to ERROR"
                    ).format(node.hostname, radosgw.display_name)
            self.send_service_alert(
                ctxt, radosgw, "service_status", "Service", "ERROR", msg,
                "SERVICE_ERROR"
            )

    def _restart_docker_service(self, ctxt, service, container_name, node):
        if self.debug_mode and not self.if_service_alert(ctxt, node=node):
            return
        logger.info("Try to restart docker service: %s", container_name)
        service.status = \
            s_fields.ServiceStatus.STARTING
        service.save()
        msg = _("Node {}：service {} status is down, trying to restart")\
            .format(node.hostname, service.name)
        self.send_websocket(ctxt, service, "SERVICE_RESTART", msg)
        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        docker_tool = DockerTool(ssh)
        retry_times = 0
        while retry_times < 10:
            try:
                docker_tool.restart(container_name)
                if docker_tool.status(container_name):
                    break
            except exception.StorException as e:
                logger.error(e)
                retry_times += 1
                if retry_times == 10:
                    service.status = s_fields.ServiceStatus.ERROR
                    service.save()
                    msg = _(
                        "Node {}：service {} restart failed, mark it to error"
                    ).format(node.hostname, container_name)
                    self.send_service_alert(
                        ctxt, service, "service_status", "Service", "ERROR",
                        msg, "SERVICE_ERROR"
                    )

    def _update_object_gateway(self, ctxt, filters, status, node):
        rgws = objects.RadosgwList.get_all(ctxt, filters=filters)
        if not rgws:
            return
        rgw = rgws[0]
        if rgw.status not in [s_fields.RadosgwStatus.ACTIVE,
                              s_fields.RadosgwStatus.ERROR,
                              s_fields.RadosgwStatus.STARTING,
                              s_fields.RadosgwStatus.INACTIVE]:
            return rgw
        if (rgw.status == s_fields.RadosgwStatus.STARTING and
                status != s_fields.RadosgwStatus.ACTIVE):
            return rgw
        if (rgw.status == s_fields.RadosgwStatus.ERROR and
                status != s_fields.RadosgwStatus.ACTIVE):
            return rgw
        if (status == s_fields.RadosgwStatus.INACTIVE and
                rgw.status == s_fields.RadosgwStatus.ACTIVE):
            msg = _("Node {}: radosgw {} status is inactive"
                    ).format(node.hostname, rgw.display_name)
            self.send_service_alert(
                ctxt, rgw, "service_status", rgw.display_name, "WARN",
                msg, "SERVICE_INACTIVE")
        if status == s_fields.RadosgwStatus.ACTIVE:
            rgw.counter += 1
            if rgw.status in [s_fields.RadosgwStatus.INACTIVE,
                              s_fields.RadosgwStatus.STARTING,
                              s_fields.RadosgwStatus.ERROR]:
                msg = _("Node {}: radosgw {} status is active"
                        ).format(node.hostname, rgw.name)
                self.send_service_alert(
                    ctxt, rgw, "service_status", rgw.name, "INFO",
                    msg, "SERVICE_ACTIVE")
        logger.debug("RGW status from %s to %s", rgw.status, status)
        rgw.status = status
        rgw.save()
        return rgw

    def _update_radosgw_router(self, ctxt, filters, status, service_name,
                               node):
        if "keepalived" in service_name:
            filters["name"] = "keepalived"
        if "haproxy" in service_name:
            filters["name"] = "haproxy"
        service = objects.RouterServiceList.get_all(
            ctxt, filters=filters)
        if not service:
            return
        service = service[0]
        if service.status not in [
            s_fields.RouterServiceStatus.ACTIVE,
            s_fields.RouterServiceStatus.INACTIVE,
            s_fields.RouterServiceStatus.STARTING,
            s_fields.RouterServiceStatus.ERROR
        ]:
            return service
        if (service.status == s_fields.RouterServiceStatus.STARTING) \
                and (status != s_fields.RouterServiceStatus.ACTIVE):
            return service
        if (service.status == s_fields.RouterServiceStatus.ERROR) \
                and (status != s_fields.RouterServiceStatus.ACTIVE):
            return service
        if status == s_fields.RouterServiceStatus.INACTIVE \
                and service.status == s_fields.RouterServiceStatus.ACTIVE:
            msg = _("Node {}: radosgw router service {} status is inactive"
                    ).format(node.hostname, service.name)
            self.send_service_alert(
                ctxt, service, "service_status", service.name, "WARN",
                msg, "SERVICE_INACTIVE")
        if status == s_fields.RouterServiceStatus.ACTIVE:
            service.counter += 1
            if service.status in [s_fields.RouterServiceStatus.INACTIVE,
                                  s_fields.RouterServiceStatus.STARTING,
                                  s_fields.RouterServiceStatus.ERROR]:
                msg = _("Node {}: radosgw router service {} status is active"
                        ).format(node.hostname, service.name)
                self.send_service_alert(
                    ctxt, service, "service_status", service.name, "INFO",
                    msg, "SERVICE_ACTIVE")
        service.status = status
        service.save()
        return service

    def _update_normal_service(self, ctxt, filters, name, status, node,
                               role):
        service = objects.ServiceList.get_all(ctxt, filters=filters)
        if not service:
            if role in ["base", "role_monitor"]:
                return
            service_new = objects.Service(
                ctxt, name=name, status=status,
                node_id=node.id, cluster_id=ctxt.cluster_id,
                counter=0, role=role
            )
            service_new.create()
            return service_new
        else:
            service = service[0]
            if service.status not in [
                s_fields.ServiceStatus.ACTIVE,
                s_fields.ServiceStatus.INACTIVE,
                s_fields.ServiceStatus.STARTING,
                s_fields.ServiceStatus.ERROR
            ]:
                return service
            if ((service.status == s_fields.ServiceStatus.STARTING) and
                    (status != s_fields.ServiceStatus.ACTIVE)):
                return service
            if ((service.status == s_fields.ServiceStatus.ERROR) and
                    (status != s_fields.ServiceStatus.ACTIVE)):
                return service
            if (status == s_fields.ServiceStatus.INACTIVE and
                    service.status == s_fields.ServiceStatus.ACTIVE):
                node = objects.Node.get_by_id(ctxt, node.id)
                if not self.if_service_alert(ctxt, node=node):
                    return service
                if node.status in [s_fields.NodeStatus.DEPLOYING_ROLE,
                                   s_fields.NodeStatus.REMOVING_ROLE]:
                    return service
                msg = _("Node {}: service {} status is inactive"
                        ).format(node.hostname, service.name)
                self.send_service_alert(
                    ctxt, service, "service_status", service.name, "WARN",
                    msg, "SERVICE_INACTIVE")
            if status == s_fields.ServiceStatus.ACTIVE:
                service.counter += 1
                if service.status in [s_fields.ServiceStatus.INACTIVE,
                                      s_fields.ServiceStatus.STARTING,
                                      s_fields.ServiceStatus.ERROR]:
                    msg = _("Node {}: service {} status is active"
                            ).format(node.hostname, service.name)
                    self.send_service_alert(
                        ctxt, service, "service_status", service.name, "INFO",
                        msg, "SERVICE_ACTIVE")
            service.status = status
            service.save()
            return service

    def service_update(self, ctxt, services, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        if node.status in [s_fields.NodeStatus.DELETING]:
            logger.warning("Node status is %s, ignore service update",
                           node.status)
            return True
        if not self.if_service_alert(ctxt):
            logger.warning("The cluster is deleting, ignore service update")
            return True
        services = json.loads(services)
        logger.info('Update service status for node %s', node_id)
        for role, sers in six.iteritems(services):
            for s in sers:
                name = s.get('name')
                filters = {
                    "name": name,
                    "node_id": s['node_id']
                }
                status = s.get('status')
                node_id = s.get('node_id')
                service_name = s.get('service_name')
                if role == "role_object_gateway":
                    rgw = self._update_object_gateway(
                        ctxt, filters, status, node)
                    if not rgw:
                        continue
                    if rgw.status == s_fields.RadosgwStatus.INACTIVE:
                        self.task_submit(self._restart_radosgw_service,
                                         ctxt, rgw, node)
                elif role == "role_radosgw_router":
                    service = self._update_radosgw_router(
                        ctxt, filters, status, service_name, node)
                    if not service:
                        continue
                    if service.status == s_fields.RouterServiceStatus.INACTIVE:
                        self.task_submit(self._restart_docker_service,
                                         ctxt, service, service_name, node)
                else:
                    service = self._update_normal_service(
                        ctxt, filters, name, status, node, role)
                    if not service:
                        continue
                    if service.status == s_fields.ServiceStatus.INACTIVE:
                        if role in self.container_roles:
                            self.task_submit(self._restart_docker_service,
                                             ctxt, service, service_name, node)
                        else:
                            self.task_submit(self._restart_systemd_service,
                                             ctxt, name, service, node)
        return True

    def service_infos_get(self, ctxt, node):
        logger.info("Get uncertain service for node %s", node.id)
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
