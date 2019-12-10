from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields

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
            res = agent_client.ceph_services_restart(
                ctxt, "mon", node.hostname)
            return res
        else:
            raise exception.NodeNotFound(id)

    def _mgr_restart(self, ctxt, id, agent_client):
        node = objects.Node.get_by_id(ctxt, id)
        if node:
            res = agent_client.ceph_services_restart(
                ctxt, "mgr", node.hostname)
            return res
        else:
            raise exception.NodeNotFound(id)

    def _osd_restart(self, ctxt, id, agent_client):
        osd = objects.Osd.get_by_id(ctxt, id)
        if osd:
            res = agent_client.ceph_services_restart(ctxt, "osd", osd.osd_id)
            return res
        else:
            raise exception.OsdNotFound(id)

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
