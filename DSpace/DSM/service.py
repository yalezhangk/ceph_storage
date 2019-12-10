import json

import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
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

    def _restart_systemd_service(self, ctxt, name, service):
        if self.debug_mode:
            return
        logger.info("Try to restart systemd service: %s", name)
        service.status = s_fields.ServiceStatus.STARTING
        service.save()
        node = objects.Node.get_by_id(ctxt, service.node_id)
        if name == "MON":
            target = node.hostname
            type = "mon"
        elif name == "MGR":
            target = node.hostname
            type = "mgr"
        else:
            logger.error("Service not support")
            return
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

    def _restart_radosgw_service(self, ctxt, radosgw):
        if self.debug_mode:
            return
        logger.info("Try to restart rgw service: %s", radosgw.name)
        radosgw.status = s_fields.RadosgwStatus.STARTING
        radosgw.save()
        target = radosgw.name
        type = "rgw"
        node = objects.Node.get_by_id(ctxt, radosgw.node_id)
        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        ceph_tool = CephTool(ssh)
        try:
            ceph_tool.systemctl_restart(type, target)
        except exception.StorException as e:
            logger.error(e)
            radosgw.status = s_fields.RadosgwStatus.ERROR
            radosgw.save()

    def _restart_docker_service(self, ctxt, service, container_name):
        if self.debug_mode:
            return
        logger.info("Try to restart docker service: %s", container_name)
        service.status = \
            s_fields.ServiceStatus.STARTING
        service.save()
        node = objects.Node.get_by_id(ctxt, service.node_id)
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

    def _update_object_gateway(self, ctxt, filters, status):
        rgws = objects.RadosgwList.get_all(ctxt, filters=filters)
        if not rgws:
            return
        rgw = rgws[0]
        if rgw.status not in [s_fields.RadosgwStatus.ACTIVE,
                              s_fields.RadosgwStatus.ERROR,
                              s_fields.RadosgwStatus.STARTING,
                              s_fields.RadosgwStatus.INACTIVE]:
            return rgw
        if (rgw.status == s_fields.RadosgwStatus.STARTING) \
                and (status != s_fields.RadosgwStatus.ACTIVE):
            return rgw
        if (rgw.status == s_fields.RadosgwStatus.ERROR) \
                and (status != s_fields.RadosgwStatus.ACTIVE):
            return rgw
        rgw.status = status
        if status == s_fields.RadosgwStatus.ACTIVE:
            rgw.counter += 1
        rgw.save()
        return rgw

    def _update_radosgw_router(self, ctxt, filters, status, service_name):
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
        service.status = status
        if status == s_fields.RouterServiceStatus.ACTIVE:
            service.counter += 1
        service.save()
        return service

    def _update_normal_service(self, ctxt, filters, name, status, node_id,
                               role):
        service = objects.ServiceList.get_all(ctxt,
                                              filters=filters)
        if not service:
            service_new = objects.Service(
                ctxt, name=name, status=status,
                node_id=node_id, cluster_id=ctxt.cluster_id,
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
            if (service.status == s_fields.ServiceStatus.STARTING) \
                    and (status != s_fields.ServiceStatus.ACTIVE):
                return service
            if (service.status == s_fields.ServiceStatus.ERROR) \
                    and (status != s_fields.ServiceStatus.ACTIVE):
                return service
            service.status = status
            if status == s_fields.ServiceStatus.ACTIVE:
                service.counter += 1
            service.save()
            return service

    def service_update(self, ctxt, services):
        services = json.loads(services)
        logger.debug('Update service status')
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
                    rgw = self._update_object_gateway(ctxt, filters, status)
                    if rgw.status == s_fields.RadosgwStatus.INACTIVE:
                        self.task_submit(self._restart_radosgw_service,
                                         ctxt, rgw)
                elif role == "role_radosgw_router":
                    service = self._update_radosgw_router(
                        ctxt, filters, status, service_name)
                    if service.status == s_fields.RouterServiceStatus.INACTIVE:
                        self.task_submit(self._restart_docker_service,
                                         ctxt, service, service_name)
                else:
                    service = self._update_normal_service(
                        ctxt, filters, name, status, node_id, role)
                    if service.status == s_fields.ServiceStatus.INACTIVE:
                        if role in self.container_roles:
                            self.task_submit(self._restart_docker_service,
                                             ctxt, service, service_name)
                        else:
                            self.task_submit(self._restart_systemd_service,
                                             ctxt, name, service)
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
