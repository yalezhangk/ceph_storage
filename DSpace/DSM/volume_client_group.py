from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class VolumeClientGroupHandler(AdminBaseHandler):
    def volume_client_group_get_all(self, ctxt, marker=None, limit=None,
                                    sort_keys=None, sort_dirs=None,
                                    filters=None, offset=None,
                                    expected_attrs=None):
        return objects.VolumeClientGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def volume_client_group_get_count(self, ctxt, filters=None):
        return objects.VolumeClientGroupList.get_count(
            ctxt, filters=filters)

    def volume_client_group_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
        })
        v = objects.VolumeClientGroup(ctxt, **data)
        v.create()
        return v

    def volume_client_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
        })
        v = objects.VolumeClient(ctxt, **data)
        v.create()
        return v

    def volume_client_group_get(self, ctxt, group_id):
        return objects.VolumeClientGroup.get_by_id(ctxt, group_id)

    def volume_client_group_delete(self, ctxt, group_id):
        filters = {"volume_client_group_id": group_id}
        # delete volume clients of the volume client group
        for vc in objects.VolumeClientList.get_all(ctxt, filters=filters):
            vc.destroy()
        volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, group_id)
        volume_client_group.destroy()
        return volume_client_group

    def volume_client_get_all(self, ctxt, marker=None, limit=None,
                              sort_keys=None, sort_dirs=None,
                              filters=None, offset=None):
        filters = filters or {}
        return objects.VolumeClientList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
