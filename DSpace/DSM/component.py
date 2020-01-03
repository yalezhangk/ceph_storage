from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource

logger = logging.getLogger(__name__)


class ComponentHandler(AdminBaseHandler):

    service_list = ["mon", "mgr", "osd", "rgw", "mds"]

    def _get_mgr_mon_mds_list(self, ctxt):
        filters = {'status': s_fields.NodeStatus.ACTIVE,
                   'role_monitor': True}
        mgr_mons = objects.NodeList.get_all(ctxt, filters=filters)
        return mgr_mons

    def _get_osd_list(self, ctxt):
        osds = objects.OsdList.get_all(ctxt)
        return osds

    def _get_rgw_list(self, ctxt):
        filters = {'status': s_fields.NodeStatus.ACTIVE,
                   'role_object_gateway': True}
        rgws = objects.NodeList.get_all(ctxt, filters=filters)
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

    def _rgw_restart(self, ctxt, id):
        # TODO rgw尚未开发，数据库无数据
        pass

    def _mds_restart(self, ctxt, id):
        # TODO mds尚未开发，数据库无数据
        pass

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

    def component_restart(self, ctxt, component):
        service = component.get('service')
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
        filters = {'status': s_fields.NodeStatus.ACTIVE,
                   'role_monitor': True}
        if service == "osd":
            osd = objects.Osd.get_by_id(ctxt, component.get("id"))
            node_id = osd.node_id
        else:
            mon = objects.NodeList.get_all(ctxt, filters=filters)
            node_id = int(mon[0].id)
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=node_id)
        res = restart[service](ctxt, component.get("id"), client)
        return res

    def _rgw_start_op(self, ctxt, radosgw, begin_action=None):
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=radosgw.node_id)
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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, radosgw, op_status, msg)

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
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=radosgw.node_id)
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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, radosgw, op_status, msg)

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
