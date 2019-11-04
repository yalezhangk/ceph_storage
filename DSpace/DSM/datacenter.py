import uuid

from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class DatacenterHandler(AdminBaseHandler):
    def datacenter_create(self, ctxt):
        uid = str(uuid.uuid4())
        datacenter_name = "datacenter-{}".format(uid[0:8])
        datacenter = objects.Datacenter(
            ctxt, cluster_id=ctxt.cluster_id,
            name=datacenter_name
        )
        datacenter.create()
        return datacenter

    def datacenter_get(self, ctxt, datacenter_id):
        return objects.Datacenter.get_by_id(ctxt, datacenter_id)

    def datacenter_delete(self, ctxt, datacenter_id):
        datacenter = self.datacenter_get(ctxt, datacenter_id)
        datacenter.destroy()
        return datacenter

    def datacenter_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.DatacenterList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def datacenter_update(self, ctxt, id, name):
        datacenter = objects.Datacenter.get_by_id(ctxt, id)
        datacenter.name = name
        datacenter.save()
        return datacenter

    def datacenter_tree(self, ctxt):
        # nodes
        nodes = objects.NodeList.get_all(ctxt)
        rack_ids = {}
        for node in nodes:
            if node.rack_id not in rack_ids:
                rack_ids[node.rack_id] = []
            rack_ids[node.rack_id].append(node)

        # racks
        racks = objects.RackList.get_all(ctxt)
        dc_ids = {}
        for rack in racks:
            if rack.datacenter_id not in dc_ids:
                dc_ids[rack.datacenter_id] = []
            dc_ids[rack.datacenter_id].append(rack)
            rack.nodes = rack_ids.get(rack.id, [])

        # datacenters
        dcs = objects.DatacenterList.get_all(ctxt)
        for dc in dcs:
            dc.racks = dc_ids.get(dc.id, [])
        return dcs
