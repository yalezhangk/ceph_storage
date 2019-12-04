from oslo_log import log as logging

from DSpace import objects
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
