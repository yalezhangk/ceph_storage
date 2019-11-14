from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.taskflows.node import NodeTask

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

    def volume_client_group_create(self, ctxt, client_group):
        v = objects.VolumeClientGroup(
            ctxt,
            name=client_group.get('name'),
            type=client_group.get('type'),
            chap_enable=False,
            cluster_id=ctxt.cluster_id
        )
        v.create()
        return v

    def volume_client_create(self, ctxt, volume_client):
        v = objects.VolumeClient(
            ctxt,
            iqn=volume_client.get('iqn'),
            client_type=volume_client.get('type'),
            volume_client_group_id=volume_client.get('volume_client_group_id'),
            cluster_id=ctxt.cluster_id
        )
        v.create()
        return v

    def volume_client_group_get(self, ctxt, group_id, expected_attrs=None):
        return objects.VolumeClientGroup.get_by_id(
            ctxt, group_id, expected_attrs=expected_attrs)

    def volume_client_group_delete(self, ctxt, group_id):
        filters = {"volume_client_group_id": group_id}
        # delete volume clients of the volume client group
        # TODO 当客户端组被映射时不能删除
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

    def volume_client_group_update_name(self, ctxt, id, client_group):
        name = client_group.get("name")
        volume_client_group = objects.VolumeClientGroup.get_by_id(ctxt, id)
        if volume_client_group.name != name:
            logger.info("update client group from %s to %s",
                        volume_client_group.name, name)
            volume_client_group.name = name
            volume_client_group.save()
        return volume_client_group

    def _client_group_update_direct(self, ctxt, client_group_id,
                                    old_volume_client_ids,
                                    new_volume_clients):
        for old_volume_client_id in old_volume_client_ids:
            old_volume_client = objects.VolumeClient.get_by_id(
                ctxt, old_volume_client_id)
            logger.info("delete old volume client: %s", old_volume_client.iqn)
            old_volume_client.destroy()
        for vol_client in new_volume_clients:
            v = objects.VolumeClient(
                ctxt,
                iqn=vol_client.get('iqn'),
                client_type=vol_client.get('type'),
                volume_client_group_id=client_group_id,
                cluster_id=ctxt.cluster_id)
            logger.info("create new volume client: %s", v)
            v.create()

    def _client_group_update_mapping(self, ctxt, access_path, nodes,
                                     volumes, old_volume_clients,
                                     updated_volume_clients):
        for node in nodes:
            try:
                logger.info("trying to updating volume mappings")
                task = NodeTask(ctxt, node)
                task.bgw_change_client_group(ctxt, access_path, volumes,
                                             old_volume_clients,
                                             updated_volume_clients)
            except exception.StorException as e:
                logger.error("volume_access_path %s update client group"
                             " error: %s", e)
                raise e

    def volume_client_group_update(self, ctxt, id, new_volume_clients):
        volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, id, expected_attrs=["volume_access_paths",
                                      "volume_clients"])
        old_volume_clients = volume_client_group.volume_clients
        old_volume_client_ids = [i.id for i in old_volume_clients]
        volume_access_paths = volume_client_group.volume_access_paths
        self._client_group_update_direct(
            ctxt, id, old_volume_client_ids, new_volume_clients)
        if not volume_access_paths:
            logger.info("no access_path mapping to this client group, "
                        "just update clients for volume_client_group %s",
                        volume_client_group.name)
            return volume_client_group
        updated_volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, id, expected_attrs=["volume_clients", "volumes"])
        updated_volume_clients = updated_volume_client_group.volume_clients
        logger.debug("old_volume_clients: %s, updated_vol_clients: %s",
                     old_volume_clients, updated_volume_clients)
        updated_volumes = updated_volume_client_group.volumes
        volume_ids = [i.id for i in updated_volumes]
        volumes = []
        for volume_id in volume_ids:
            volume = objects.Volume.get_by_id(
                ctxt, volume_id, expected_attrs=["pool"])
            volumes.append(volume)
        access_path_ids = [i.id for i in volume_access_paths]
        for access_path_id in access_path_ids:
            access_path = objects.VolumeAccessPath.get_by_id(
                ctxt, access_path_id, expected_attrs=["volume_gateways",
                                                      "nodes"])
            nodes = access_path.nodes
            self._client_group_update_mapping(
                ctxt, access_path, nodes, volumes, old_volume_clients,
                updated_volume_clients)

    def _set_mutual_chap(self, ctxt, access_path, nodes, volume_clients,
                         mutual_chap_enable, mutual_username, mutual_password):
        for node in nodes:
            try:
                logger.info("trying to set mutual chap for %s",
                            access_path.name)
                task = NodeTask(ctxt, node)
                task.bgw_set_mutual_chap(ctxt, access_path, volume_clients,
                                         mutual_chap_enable, mutual_username,
                                         mutual_password)
            except exception.StorException as e:
                logger.error("set mutual chap error %s", e)
                raise e

    def set_mutual_chap(self, ctxt, id, data):
        mutual_chap_enable = data.get("mutual_chap_enable")
        mutual_username = data.get("mutual_username")
        mutual_password = data.get("mutual_password")
        volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, id, expected_attrs=["volume_access_paths", "volume_clients"])
        volume_clients = volume_client_group.volume_clients
        access_paths = volume_client_group.volume_access_paths
        if not access_paths:
            logger.error("no access path mapping to this client group: %s",
                         volume_client_group.name)
            raise exception.VolumeClientGroupNoMapping(
                volume_client_group=volume_client_group.name)
        access_path_ids = [i.id for i in access_paths]
        for access_path_id in access_path_ids:
            access_path = objects.VolumeAccessPath.get_by_id(
                ctxt, access_path_id, expected_attrs=["volume_gateways",
                                                      "nodes"])
            if not access_path.chap_enable:
                logger.error("access_path <%s> don't enable chap, mutual chap"
                             " will not enable", access_path.name)
                raise exception.VolumeClientGroupEnableChapError(
                    access_path=access_path.name,
                    volume_client_group=volume_client_group.name)
            nodes = access_path.nodes
            self._set_mutual_chap(ctxt, access_path, nodes, volume_clients,
                                  mutual_chap_enable, mutual_username,
                                  mutual_password)
        volume_client_group.chap_enable = mutual_chap_enable
        volume_client_group.chap_username = mutual_username
        volume_client_group.chap_password = mutual_password
        volume_client_group.save()
        return volume_client_group
