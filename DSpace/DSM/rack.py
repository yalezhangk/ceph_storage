import uuid

from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

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

    def rack_get(self, ctxt, rack_id):
        return objects.Rack.get_by_id(ctxt, rack_id)

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

    def rack_update_name(self, ctxt, id, name):
        rack = objects.Rack.get_by_id(ctxt, id)
        rack.name = name
        rack.save()
        return rack

    def rack_have_osds(self, ctxt, rack_id):
        # TODO 检查机架中的OSD是否在一个存储池中
        pass

    def rack_update_toplogy(self, ctxt, id, datacenter_id):
        rack = objects.Rack.get_by_id(ctxt, id)
        if self.rack_have_osds(ctxt, id):
            return rack
        rack.datacenter_id = datacenter_id
        rack.save()
        return rack
