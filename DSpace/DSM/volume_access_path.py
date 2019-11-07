import time
import uuid

from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class VolumeAccessPathHandler(AdminBaseHandler):
    def volume_access_path_get_all(self, ctxt, marker=None, limit=None,
                                   sort_keys=None, sort_dirs=None,
                                   filters=None, offset=None,
                                   expected_attrs=None):
        return objects.VolumeAccessPathList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def volume_access_path_get_count(self, ctxt, filters=None):
        return objects.VolumeAccessPathList.get_count(
            ctxt, filters=filters)

    def volume_access_path_get(self, ctxt, volume_access_path_id,
                               expected_attrs=None):
        return objects.VolumeAccessPath.get_by_id(
            ctxt, volume_access_path_id, expected_attrs=expected_attrs)

    def _gen_iqn(self):
        IQN_PREFIX = "iqn.%(date)s.dspace.net:%(uuid)s"
        iqn = IQN_PREFIX % {'date': time.strftime("%Y-%m", time.gmtime()),
                            'uuid': str(uuid.uuid4()).split('-')[-1]}
        return iqn

    def volume_access_path_create(self, ctxt, data):
        iqn = self._gen_iqn()
        access_path = objects.VolumeAccessPath(
            ctxt,
            name=data.get('name'),
            iqn=iqn,
            status=s_fields.VolumeAccessPathStatus.ACTIVE,
            type=data.get('type'),
            chap_enable=False,
            cluster_id=ctxt.cluster_id
        )
        access_path.create()
        return access_path

    def volume_access_path_update(self, ctxt, id, data):
        volume_access_path = objects.VolumeAccessPath.get_by_id(ctxt, id)
        volume_access_path.name = data.get("name")
        volume_access_path.save()
        return volume_access_path

    def volume_access_path_delete(self, ctxt, id):
        volume_access_path = objects.VolumeAccessPath.get_by_id(ctxt, id)
        volume_access_path.destroy()
        return volume_access_path
