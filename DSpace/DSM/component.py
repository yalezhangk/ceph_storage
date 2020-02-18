from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource

logger = logging.getLogger(__name__)


class ComponentHandler(AdminBaseHandler):

    service_list = ["mon", "mgr", "osd", "rgw", "mds"]
    monitor_service = ["mon", "mgr", "mds"]

    def _get_mgr_mon_mds_list(self, ctxt):
        filters = {'role_monitor': True}
        mgr_mons = objects.NodeList.get_all(ctxt, filters=filters)
        return mgr_mons

    def _get_osd_list(self, ctxt):
        osds = objects.OsdList.get_all(ctxt)
        return osds

    def _get_rgw_list(self, ctxt):
        rgws = objects.RadosgwList.get_all(ctxt)
        return rgws

    def _mon_restart(self, ctxt, id, agent_client):
        node = objects.Node.get_by_id(ctxt, id)
        if node:
            begin_action = self.begin_action(
                ctxt, Resource.NODE, Action.MON_RESTART, node)
            res = agent_client.ceph_services_restart(
                ctxt, "mon", node.hostname)
            self.finish_action(begin_action, id, node.hostname, node)
            return res
        else:
            raise exception.NodeNotFound(id)

    def _mgr_restart(self, ctxt, id, agent_client):
        node = objects.Node.get_by_id(ctxt, id)
        if node:
            begin_action = self.begin_action(
                ctxt, Resource.NODE, Action.MGR_RESTART, node)
            res = agent_client.ceph_services_restart(
                ctxt, "mgr", node.hostname)
            self.finish_action(begin_action, id, node.hostname, node)
            return res
        else:
            raise exception.NodeNotFound(id)

    def _osd_restart(self, ctxt, id, agent_client):
        osd = objects.Osd.get_by_id(ctxt, id)
        if osd.status in s_fields.OsdStatus.OPERATION_STATUS:
            raise exception.InvalidInput(_("Osd is {} status, can not "
                                           "restart".format(osd.status)))
        else:
            begin_action = self.begin_action(
                ctxt, Resource.OSD, Action.OSD_RESTART, osd)
            res = agent_client.ceph_services_restart(ctxt, "osd", osd.osd_id)
            self.finish_action(begin_action, id, osd.osd_name, osd)
            return res

    def _rgw_restart(self, ctxt, id, agent_client):
        rgw = objects.Radosgw.get_by_id(ctxt, id)
        if rgw:
            begin_action = self.begin_action(
                ctxt, Resource.RADOSGW, Action.RGW_RESTART, rgw)
            res = agent_client.ceph_services_restart(
                ctxt, "rgw", rgw.name)
            self.finish_action(begin_action, id, rgw.display_name, rgw)
            return res
        else:
            raise exception.NodeNotFound(id)

    def _mds_restart(self, ctxt, id, agent_client):
        node = objects.Node.get_by_id(ctxt, id)
        if node:
            begin_action = self.begin_action(
                ctxt, Resource.NODE, Action.MDS_RESTART, node)
            res = agent_client.ceph_services_restart(
                ctxt, "mds", node.hostname)
            self.finish_action(begin_action, id, node.hostname, node)
            return res
        else:
            raise exception.NodeNotFound(id)

    def components_get_list(self, ctxt, services=None):
        res = {}
        if not services:
            services = self.service_list
        for service in services:
            if service not in self.service_list:
                logger.error("service type not supported: %s" % service)
                continue
            logger.info("Get %s cmponent list." % services)
            _service = service
            if service in ["mon", "mgr", "mds"]:
                service = "mgr_mon_mds"
            res.update(
                {_service: getattr(self, '_get_%s_list' % service)(ctxt)})
        return res

    def _get_service_map_info(self, ctxt, service, id):
        role = None
        key = None
        value = None
        if service in self.monitor_service:
            role = "role_monitor"
            key = service.upper()
            value = self.map_util.role_monitor[service.upper()]
        elif service == "rgw":
            rgw = objects.Radosgw.get_by_id(ctxt, id)
            role = "role_object_gateway"
            key = rgw.name
            value = "ceph-radosgw@rgw.{}.service".format(rgw.name)
        elif service == "osd":
            role = "role_storage"
        return role, key, value

    def _ignore_service_check(self, ctxt, service, id, role, key, client):
        if service == "osd":
            osd = objects.Osd.get_by_id(ctxt, id)
            if not self.ignored_osds.get(ctxt.cluster_id):
                self.ignored_osds.update({ctxt.cluster_id: [osd.osd_id]})
            else:
                self.ignored_osds[ctxt.cluster_id].append(osd.osd_id)
        else:
            client.service_map_remove(ctxt, role, key)

    def _recover_service_check(self, ctxt, service, id, role, key, value,
                               client):
        if service == "osd":
            osd = objects.Osd.get_by_id(ctxt, id)
            if not self.ignored_osds.get(ctxt.cluster_id):
                self.ignored_osds.update({ctxt.cluster_id: [osd.osd_id]})
            else:
                self.ignored_osds[ctxt.cluster_id].append(osd.osd_id)
        else:
            client.service_map_add(ctxt, role, key, value)

    def component_restart(self, ctxt, component):
        service = component.get('service')
        id = component.get("id")
        if service not in self.service_list:
            logger.error("service type not supported: %s" % service)
            return "service type not supported: %s" % service
        restart = {
            "mon": self._mon_restart,
            "mgr": self._mgr_restart,
            "osd": self._osd_restart,
            "rgw": self._rgw_restart,
            "mds": self._mds_restart
        }
        if service == "osd":
            osd = objects.Osd.get_by_id(ctxt, id)
            node_id = osd.node_id
        elif service == "rgw":
            rgw = objects.Radosgw.get_by_id(ctxt, id)
            node_id = rgw.node_id
        else:
            filters = {'role_monitor': True}
            mon = objects.NodeList.get_all(ctxt, filters=filters)
            node_id = int(mon[0].id)
        node = objects.Node.get_by_id(ctxt, node_id)
        self.check_agent_available(ctxt, node)
        client = self.agent_manager.get_client(node_id)

        role, key, value = self._get_service_map_info(ctxt, service, id)
        self._ignore_service_check(ctxt, service, id, role, key, client)

        res = restart[service](ctxt, component.get("id"), client)

        self._recover_service_check(
            ctxt, service, id, role, key, value, client)
        return res

    def _rgw_start_op(self, ctxt, radosgw, begin_action=None):
        client = self.agent_manager.get_client(node_id=radosgw.node_id)
        try:
            client.ceph_services_start(ctxt, "rgw", radosgw.name)
            node = objects.Node.get_by_id(ctxt, radosgw.node_id)
            self.notify_node_update(ctxt, node)
            status = s_fields.RadosgwStatus.ACTIVE
            radosgw.status = status
            radosgw.save()
            logger.info("client.rgw.%s start success", radosgw.name)
            op_status = 'START_SUCCESS'
            msg = _("Start success: {}").format(radosgw.display_name)
            err_msg = None
        except exception.StorException as e:
            logger.error(e)
            err_msg = str(e)
            status = s_fields.RadosgwStatus.ERROR
            radosgw.status = status
            radosgw.save()
            logger.info("client.rgw.%s start failed", radosgw.name)
            op_status = 'START_FAILD'
            msg = _("Start failed: {}").format(radosgw.display_name)
        self.finish_action(
            begin_action, radosgw.id, radosgw.display_name, radosgw,
            status=status, err_msg=err_msg)
        self.send_websocket(ctxt, radosgw, op_status, msg)

    def _rgw_start(self, ctxt, radosgw_id):
        radosgw = objects.Radosgw.get_by_id(ctxt, radosgw_id)
        if not radosgw:
            raise exception.RadosgwNotFound(radosgw_id=radosgw_id)
        if radosgw.status not in [s_fields.RadosgwStatus.INACTIVE,
                                  s_fields.RadosgwStatus.STOPPED,
                                  s_fields.RadosgwStatus.ERROR]:
            logger.error("Service status is %s, cannot start", radosgw.status)
            return ("Service status is %s, cannot start", radosgw.status)
        begin_action = self.begin_action(
            ctxt, Resource.RADOSGW, Action.RGW_START, radosgw)
        radosgw.status = s_fields.RadosgwStatus.STARTING
        radosgw.save()
        self.task_submit(self._rgw_start_op, ctxt, radosgw, begin_action)
        return radosgw

    def component_start(self, ctxt, component):
        logger.info("Start service with data %s", component)
        service = component.get('service')
        if service not in self.service_list:
            logger.error("service type not supported: %s" % service)
            return "service type not supported: %s" % service
        start = {
            "rgw": self._rgw_start,
        }
        res = start[service](ctxt, component.get("id"))
        return res

    def _rgw_stop_op(self, ctxt, radosgw, begin_action=None):
        client = self.agent_manager.get_client(node_id=radosgw.node_id)
        try:
            client.ceph_services_stop(ctxt, "rgw", radosgw.name)
            radosgw.status = s_fields.RadosgwStatus.STOPPED
            radosgw.save()
            node = objects.Node.get_by_id(ctxt, radosgw.node_id)
            self.notify_node_update(ctxt, node)
            logger.info("client.rgw.%s stop success", radosgw.name)
            op_status = 'STOP_SUCCESS'
            msg = _("Stop success: {}").format(radosgw.display_name)
            status = 'success'
            err_msg = None
        except exception.StorException as e:
            status = 'fail'
            err_msg = str(e)
            logger.error(e)
            radosgw.status = s_fields.RadosgwStatus.ERROR
            radosgw.save()
            logger.info("client.rgw.%s stop failed", radosgw.name)
            op_status = 'STOP_FAILD'
            msg = _("Stop failed: {}").format(radosgw.display_name)

        self.finish_action(
            begin_action, radosgw.id, radosgw.display_name, radosgw,
            status=status, err_msg=err_msg)
        self.send_websocket(ctxt, radosgw, op_status, msg)

    def _rgw_stop(self, ctxt, radosgw_id):
        radosgw = objects.Radosgw.get_by_id(ctxt, radosgw_id)
        if not radosgw:
            raise exception.RadosgwNotFound(radosgw_id=radosgw_id)
        if radosgw.status not in [s_fields.RadosgwStatus.ACTIVE,
                                  s_fields.RadosgwStatus.ERROR]:
            logger.error("Service status is %s, cannot stop", radosgw.status)
            return ("Service status is %s, cannot stop", radosgw.status)
        begin_action = self.begin_action(
            ctxt, Resource.RADOSGW, Action.RGW_STOP, radosgw)
        radosgw.status = s_fields.RadosgwStatus.STOPPING
        radosgw.save()
        self.task_submit(self._rgw_stop_op, ctxt, radosgw, begin_action)
        return radosgw

    def component_stop(self, ctxt, component):
        logger.info("Stop service with data %s", component)
        service = component.get('service')
        if service not in self.service_list:
            logger.error("service type not supported: %s" % service)
            return "service type not supported: %s" % service
        start = {
            "rgw": self._rgw_stop,
        }
        res = start[service](ctxt, component.get("id"))
        return res
