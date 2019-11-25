import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class RackHandler(AdminBaseHandler):
    def rack_create(self, ctxt, datacenter_id):
        uid = str(uuid.uuid4())
        rack_name = "rack-{}".format(uid[0:8])
        rack = objects.Rack(
            ctxt, cluster_id=ctxt.cluster_id,
            datacenter_id=datacenter_id,
            name=rack_name
        )
        rack.create()
        return rack

    def rack_get(self, ctxt, rack_id, expected_attrs=None):
        return objects.Rack.get_by_id(ctxt, rack_id,
                                      expected_attrs=expected_attrs)

    def rack_delete(self, ctxt, rack_id):
        rack = self.rack_get(ctxt, rack_id)
        rack.destroy()
        return rack

    def rack_get_all(self, ctxt, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        return objects.RackList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def _check_rack_by_name(self, ctxt, name):
        filters = {"name": name}
        v = self.rack_get_all(ctxt, filters=filters)
        if v:
            logger.error("update rack error, %s already exists",
                         name)
            raise exception.Duplicate(
                _("rack: {} is already exists!").format(name))

    def rack_update_name(self, ctxt, id, name):
        rack = objects.Rack.get_by_id(ctxt, id)
        self._check_rack_by_name(ctxt, name)
        rack.name = name
        rack.save()
        return rack

    def rack_update_toplogy(self, ctxt, id, datacenter_id):
        rack = objects.Rack.get_by_id(ctxt, id, expected_attrs=['nodes'])
        rack_nodes = rack.nodes
        logger.info("rack %s has nodes: %s", rack.name, rack_nodes)
        for rack_node in rack_nodes:
            node = objects.Node.get_by_id(ctxt, rack_node.id,
                                          expected_attrs=['osds'])
            osd_crush_rule_ids = [i.crush_rule_id for i in node.osds]
            logger.info("rack_update_toplogy, osd_crush_rule_ids: %s",
                        osd_crush_rule_ids)
            if any(osd_crush_rule_ids):
                logger.error("node %s has osds already in a pool, can't move",
                             node.hostname)
                raise exception.RackMoveNotAllow(rack=rack.name)
        rack.datacenter_id = datacenter_id
        rack.save()
        return rack
