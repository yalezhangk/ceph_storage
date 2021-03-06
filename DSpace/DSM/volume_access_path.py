import time
import uuid

from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.taskflows.node import NodeTask
from DSpace.utils.coordination import synchronized

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
        iqn_prefix = "iqn.%(date)s.dspace.net:%(uuid)s"
        iqn = iqn_prefix % {'date': time.strftime("%Y-%m", time.gmtime()),
                            'uuid': str(uuid.uuid4()).split('-')[-1]}
        return iqn

    def volume_access_path_create(self, ctxt, data):
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.CREATE)
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
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _check_volume_access_path_name(self, ctxt, name):
        filters = {"name": name}
        v = self.volume_access_path_get_all(ctxt, filters=filters)
        if v:
            logger.error("update access_path error, %s already exists",
                         name)
            raise exception.Duplicate(
                    _("volume_access_path: {} is"
                      "already exists!").format(name))

    def volume_access_path_update(self, ctxt, id, data):
        volume_access_path = objects.VolumeAccessPath.get_by_id(ctxt, id)
        if not volume_access_path:
            logger.error("access path<%s> not found", id)
            raise exception.VolumeAccessPathNotFound(access_path_id=id)
        if volume_access_path.name != data.get('name'):
            self._check_volume_access_path_name(ctxt, data.get('name'))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.UPDATE,
            before_obj=volume_access_path
        )
        volume_access_path.name = data.get("name")
        volume_access_path.save()
        self.finish_action(begin_action, resource_id=volume_access_path.id,
                           resource_name=volume_access_path.name,
                           after_obj=volume_access_path)
        return volume_access_path

    def volume_access_path_delete(self, ctxt, id):
        volume_access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways",
                                      "volume_client_groups"])
        if volume_access_path.volume_gateways:
            logger.error("access path %s has mounted gateway, can't delete",
                         volume_access_path.name)
            raise exception.AccessPathDeleteError(
                reason=_("has mounted gateway"))
        if volume_access_path.volume_client_groups:
            logger.error("access path %s has volume mappings, can't delete",
                         volume_access_path.name)
            raise exception.AccessPathDeleteError(
                reason=_("has volume mappings"))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.DELETE,
            before_obj=volume_access_path)
        volume_access_path.destroy()
        self.finish_action(begin_action, resource_id=volume_access_path.id,
                           resource_name=volume_access_path.name,
                           after_obj=volume_access_path)
        return volume_access_path

    def volume_access_path_mount_gw(self, ctxt, id, data):
        expected_attrs = ['volume_gateways', 'volume_client_groups',
                          'nodes', 'volumes', 'volume_clients']
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=expected_attrs)
        node_id = data.get("node_id")
        node = objects.Node.get_by_id(ctxt, node_id)
        volume_gateways = access_path.volume_gateways
        gateway_node_ids = [i.node_id for i in volume_gateways]
        logger.debug("get gateway_node_ids %s filter by "
                     "access_path %s", gateway_node_ids, access_path)
        logger.debug("access_path.volume_gateways: %s",
                     access_path.volume_gateways)
        if node_id in gateway_node_ids:
            logger.error("node %s already has volume gateway filter by "
                         "access_path %s", node.hostname, access_path.name)
            raise exception.VolumeGatewayExists(node=node.hostname)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_MOUNT_GW,
            before_obj=access_path)
        logger.debug("mount bgw in node: <%s>%s for access path: %s",
                     node_id, node, access_path)
        ap_gateway = objects.VolumeGateway(
            ctxt,
            node_id=node_id,
            volume_access_path_id=id,
            cluster_id=ctxt.cluster_id
        )
        volume_ids = [i.id for i in access_path.volumes]
        volumes = []
        for volume_id in volume_ids:
            volume = objects.Volume.get_by_id(
                ctxt, volume_id, expected_attrs=["pool"])
            volumes.append(volume)
        volume_clients = access_path.volume_clients

        try:
            task = NodeTask(ctxt, node)
            logger.info("trying to mount block gateway")
            task.mount_bgw(ctxt, access_path, node)
            ap_gateway.create()
            access_path.volume_gateway_append(
                volume_gateway_id=ap_gateway.id)
            # access path already has mappings
            if volumes and volume_clients:
                for volume_client in volume_clients:
                    self._create_mapping(
                        ctxt, [node_id], access_path, volume_client, volumes)
        except exception.StorException as e:
            logger.error("mount bgw error: %s", e)
            raise e
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    @synchronized("volume_gateway-{id}")
    def volume_access_path_unmount_gw(self, ctxt, id, data):
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])
        node_id = data.get("node_id")
        volume_gateways = access_path.volume_gateways
        if not volume_gateways:
            logger.error("access path %s has no more gateway, can't unmount",
                         access_path.name)
            raise exception.AccessPathUnmountBgwError(
                reason=_("no more gateway to delete"))
        gateway_ids = [i.id for i in volume_gateways]
        logger.debug("access_path.volume_gateways: %s",
                     access_path.volume_gateways)
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={"volume_access_path_id": access_path.id})
        if volume_mappings:
            logger.error("access path %s has volume mappings, can't unmount "
                         "bgw", access_path.name)
            raise exception.AccessPathUnmountBgwError(
                reason=_("has volume mappings attached"))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_UNMOUNT_GW,
            before_obj=access_path)
        for gateway_id in gateway_ids:
            ap_gateway = objects.VolumeGateway.get_by_id(ctxt, gateway_id)
            if ap_gateway.node_id == node_id:
                access_path.volume_gateway_remove(
                    volume_gateway_id=ap_gateway.id)
                ap_gateway.destroy()
        try:
            node = objects.Node.get_by_id(ctxt, node_id)
            task = NodeTask(ctxt, node)
            task.unmount_bgw(ctxt, access_path)
            self.finish_action(begin_action, resource_id=access_path.id,
                               resource_name=access_path.name,
                               after_obj=access_path)
        except exception.StorException as e:
            logger.error("unmount bgw error: %s", e)
            self.finish_action(begin_action, resource_id=access_path.id,
                               resource_name=access_path.name,
                               after_obj=access_path, status='error',
                               err_msg=str(e))
            raise e
        return access_path

    def volume_access_path_set_chap(self, ctxt, id, data):
        chap_enable = data.get("chap_enable")
        username = data.get("username")
        password = data.get("password")
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])
        volume_gateways = access_path.volume_gateways
        if not volume_gateways:
            logger.error("access_path %s has no volume gateway",
                         access_path.name)
            raise exception.AccessPathNoGateway(access_path=access_path.name)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_UPDATE_CHAP,
            before_obj=access_path
        )
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={"volume_access_path_id": access_path.id})

        if volume_mappings:
            gateway_node_ids = [i.node_id for i in volume_gateways]
            for node_id in gateway_node_ids:
                try:
                    node = objects.Node.get_by_id(ctxt, node_id)
                    task = NodeTask(ctxt, node)
                    task.bgw_set_chap(ctxt, access_path, chap_enable,
                                      username, password)
                except exception.StorException as e:
                    logger.error("volume_access_path %s set "
                                 "chap error %s".format(access_path.name, e))
                    raise e
            logger.info("volume_access_path_set_chap success, %s", access_path)
        else:
            logger.info("access_path %s has no mapping, "
                        "ignore set chap to iscsi target", access_path.name)
        access_path.chap_enable = chap_enable
        access_path.chap_username = username
        access_path.chap_password = password
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _create_mapping(self, ctxt, node_ids, access_path,
                        volume_client, volumes):
        volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, volume_client.volume_client_group_id)
        mutual_chap_enable = volume_client_group.chap_enable
        mutual_username = volume_client_group.chap_username
        mutual_password = volume_client_group.chap_password
        logger.debug("volume_client_group: %s", volume_client_group)

        for node_id in node_ids:
            try:
                node = objects.Node.get_by_id(ctxt, node_id)
                task = NodeTask(ctxt, node)
                logger.debug("trying to create mapping for node: %s",
                             node.hostname)
                task.bgw_create_mapping(
                    ctxt, access_path, volume_client, volumes)
                task.bgw_set_mutual_chap(ctxt, access_path, [volume_client],
                                         mutual_chap_enable, mutual_username,
                                         mutual_password)
                logger.debug("create mapping for node: %s success",
                             node.hostname)
            except exception.StorException as e:
                logger.error("create mapping error: %s, access_path: %s",
                             access_path.name, e)
                raise e

    def volume_access_path_create_mapping(self, ctxt, id, mapping_list):
        # 1. check
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])
        volume_gateways = access_path.volume_gateways
        if not volume_gateways:
            logger.error("no volume gateway, can't create volume mapping")
            raise exception.AccessPathNoGateway(access_path=access_path.name)
        gateway_node_ids = [i.node_id for i in volume_gateways]
        new_volume_mappings = []
        client_group_map_volumes = {}
        for mapping in mapping_list:
            client_group_id = mapping.get('client_group_id')
            client_group = objects.VolumeClientGroup.get_by_id(
                ctxt, client_group_id)
            available_volumes = []
            volume_ids = mapping.get('volume_ids')
            for volume_id in volume_ids:
                volume = objects.Volume.get_by_id(
                    ctxt, volume_id,
                    expected_attrs=["pool", "volume_access_path"])
                if volume.volume_access_path:
                    # ?????????????????????????????????
                    raise exception.InvalidInput(
                        reason=_(
                            'The volume {} has related access_path').format(
                            volume.display_name))
                volume_mappings = objects.VolumeMappingList.get_all(
                    ctxt, filters={"volume_id": volume_id,
                                   "volume_access_path_id": access_path.id,
                                   "volume_client_group_id": client_group_id})
                if volume_mappings:
                    logger.error("access path %s already has mapping",
                                 access_path.name)
                    raise exception.AccessPathMappingVolumeExists(
                        access_path=access_path.name,
                        client_group=client_group.name,
                        volume=volume.volume_name)
                available_volumes.append(volume)
                volume_mapping = objects.VolumeMapping(
                    ctxt, volume_id=volume_id,
                    volume_access_path_id=access_path.id,
                    volume_client_group_id=client_group_id,
                    cluster_id=ctxt.cluster_id)
                new_volume_mappings.append(volume_mapping)
            client_group_map_volumes[client_group_id] = available_volumes
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_CREATE_MAPPING,
            before_obj=access_path)
        # 2. db create
        for new_volume_mapping in new_volume_mappings:
            new_volume_mapping.create()
        for client_group_id, volumes in client_group_map_volumes.items():
            client_group = objects.VolumeClientGroup.get_by_id(
                ctxt, client_group_id, expected_attrs=["volume_clients"])
            volume_clients = client_group.volume_clients
            for volume_client in volume_clients:
                self._create_mapping(ctxt, gateway_node_ids, access_path,
                                     volume_client, volumes)
        access_path.updated_at = timeutils.utcnow()
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _remove_mapping(self, ctxt, node_ids, access_path,
                        volume_client, volumes):
        for node_id in node_ids:
            try:
                node = objects.Node.get_by_id(ctxt, node_id)
                task = NodeTask(ctxt, node)
                task.bgw_remove_mapping(
                    ctxt, access_path, volume_client, volumes)
            except exception.StorException as e:
                logger.error("volume_access_path {} set chap error {}".format(
                    access_path.name, e))
                raise e

    def volume_access_path_remove_mapping(self, ctxt, id, mapping_list):
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])
        # 1. check
        volume_gateways = access_path.volume_gateways
        if not volume_gateways:
            logger.error("access path %s no gateway, can't remove mapping",
                         access_path.name)
            raise exception.AccessPathNoGateway(access_path=access_path.name)
        gateway_node_ids = [i.node_id for i in volume_gateways]
        volume_mappings_list = []
        volume_map_c_group = {}
        for mapping in mapping_list:
            client_group_id = mapping.get('client_group_id')
            volume_ids = mapping.get('volume_ids')
            client_group = objects.VolumeClientGroup.get_by_id(
                ctxt, client_group_id)
            volumes = []
            for volume_id in volume_ids:
                volume = objects.Volume.get_by_id(
                    ctxt, volume_id, expected_attrs=["pool"])
                volumes.append(volume)
                volume_mappings = objects.VolumeMappingList.get_all(
                    ctxt, filters={'volume_id': volume_id,
                                   'volume_access_path_id': access_path.id,
                                   'volume_client_group_id': client_group_id})
                if not volume_mappings:
                    logger.error("volume mapping not found")
                    raise exception.AccessPathNoMapping(
                        access_path=access_path.name)
                if len(volume_mappings) > 1:
                    logger.error("volume mapping must be unique")
                    raise exception.ProgrammingError(reason="get multi rows")
                volume_mappings_list.append(volume_mappings[0])
            volume_map_c_group[client_group.id] = volumes
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_REMOVE_MAPPING,
            before_obj=access_path)
        # 2. db operate
        for volume_mapping in volume_mappings_list:
            volume_mapping.destroy()
        for c_group_id, volumes in volume_map_c_group.items():
            volume_clients = objects.VolumeClientList.get_all(
                ctxt, filters={'volume_client_group_id': c_group_id})
            for volume_client in volume_clients:
                self._remove_mapping(ctxt, gateway_node_ids, access_path,
                                     volume_client, volumes)
        access_path.updated_at = timeutils.utcnow()
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _add_volume(self, ctxt, node_ids, access_path, volume_client, volumes):
        for node_id in node_ids:
            try:
                node = objects.Node.get_by_id(ctxt, node_id)
                task = NodeTask(ctxt, node)
                task.bgw_add_volume(ctxt, access_path, volume_client, volumes)
            except exception.StorException as e:
                logger.error("volume_access_path {} set chap error {}".format(
                    access_path.name, e))
                raise e

    def volume_access_path_add_volume(self, ctxt, id, data):
        volume_ids = data.get("volume_ids")
        client_group_id = data.get("client_group_id")
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])
        volume_gateways = access_path.volume_gateways
        # 1. check
        if not volume_gateways:
            logger.error("access path %s no volume gateways", access_path.name)
            raise exception.AccessPathNoGateway(access_path=access_path.name)
        gateway_node_ids = [i.node_id for i in volume_gateways]
        client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, client_group_id)
        volume_clients = objects.VolumeClientList.get_all(
            ctxt, filters={'volume_client_group_id': client_group.id})
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={"volume_access_path_id": access_path.id,
                           "volume_client_group_id": client_group_id})
        cg_vol_ids = [i.volume_id for i in volume_mappings]
        available_volumes = []
        new_volume_mapping = []
        for volume_id in volume_ids:
            volume = objects.Volume.get_by_id(
                ctxt, volume_id, expected_attrs=["pool", "volume_access_path"])
            if volume.volume_access_path:
                # ?????????????????????????????????
                raise exception.InvalidInput(
                    reason=_('The volume {} has related access_path').format(
                        volume.display_name))
            if volume_id in cg_vol_ids:
                logger.error("volume mapping %s<-->%s already has volume %s",
                             access_path.name, client_group.name, volume.name)
                raise exception.AccessPathMappingVolumeExists(
                    access_path=access_path.name,
                    client_group=client_group.name,
                    volume=volume.name)
            available_volumes.append(volume)
            volume_mapping = objects.VolumeMapping(
                ctxt, volume_id=volume_id,
                volume_access_path_id=id,
                volume_client_group_id=client_group_id,
                cluster_id=ctxt.cluster_id)
            new_volume_mapping.append(volume_mapping)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_ADD_VOLUME,
            before_obj=access_path
        )
        # 2. create db relations
        for new_mapping in new_volume_mapping:
            new_mapping.create()
        for volume_client in volume_clients:
            self._add_volume(ctxt, gateway_node_ids, access_path,
                             volume_client, available_volumes)
        access_path.updated_at = timeutils.utcnow()
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _remove_volume(self, ctxt, node_ids, access_path, volume_client,
                       volumes):
        for node_id in node_ids:
            try:
                node = objects.Node.get_by_id(ctxt, node_id)
                task = NodeTask(ctxt, node)
                task.bgw_remove_volume(
                    ctxt, access_path, volume_client, volumes)
            except exception.StorException as e:
                logger.error("volume_access_path %s remove volume "
                             "error: %s", access_path.name, e)
                raise e

    def volume_access_path_remove_volume(self, ctxt, id, data):
        client_group_id = data.get("client_group_id")
        volume_ids = data.get("volume_ids")
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways"])

        # 1. check
        volume_gateways = access_path.volume_gateways
        if not volume_gateways:
            logger.error("access path %s no volume gateways", access_path.name)
            raise exception.AccessPathNoGateway(access_path=access_path.name)
        gateway_node_ids = [i.node_id for i in volume_gateways]
        client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, client_group_id)
        volume_clients = objects.VolumeClientList.get_all(
            ctxt, filters={'volume_client_group_id': client_group.id})
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={"volume_access_path_id": access_path.id,
                           "volume_client_group_id": client_group_id})
        cg_vol_ids = [i.volume_id for i in volume_mappings]
        if not cg_vol_ids:
            logger.error("access path %s already no volumes, don't remove now")
            raise exception.AccessPathNoVolmues(access_path=access_path.name)
        volumes = []
        volume_mappings_list = []
        for volume_id in volume_ids:
            volume = objects.Volume.get_by_id(
                ctxt, volume_id, expected_attrs=["pool"])
            if volume_id not in cg_vol_ids:
                logger.error("access path %s no this volume %s, can't remove",
                             access_path.name, volume.name)
                raise exception.AccessPathNoSuchVolume(
                    access_path=access_path.name, volume=volume.name)
            volume_mappings = objects.VolumeMappingList.get_all(
                ctxt, filters={"volume_access_path_id": access_path.id,
                               "volume_client_group_id": client_group_id,
                               "volume_id": volume_id})
            if not volume_mappings:
                logger.error("volume mapping not found")
                raise exception.AccessPathNoMapping(
                    access_path=access_path.name)
            if len(volume_mappings) > 1:
                logger.error("volume mapping must be unique")
                raise exception.ProgrammingError(reason="get multi rows")
            volumes.append(volume)
            volume_mappings_list.append(volume_mappings[0])
        # 2. db operation
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_REMOVE_VOLUME,
            before_obj=access_path
        )
        for volume_mapping in volume_mappings_list:
            volume_mapping.destroy()
        for volume_client in volume_clients:
            self._remove_volume(ctxt, gateway_node_ids, access_path,
                                volume_client, volumes)
        access_path.updated_at = timeutils.utcnow()
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def _volume_access_path_change_client_group(self, ctxt, access_path,
                                                volumes, volume_clients,
                                                new_client_group):
        nodes = access_path.nodes
        for node in nodes:
            try:
                task = NodeTask(ctxt, node)
                task.bgw_change_client_group(
                    ctxt, access_path, volumes, volume_clients,
                    new_client_group)
            except exception.StorException as e:
                logger.error("volume_access_path %s change client group"
                             "error: %s", access_path.name, e)
                raise e

    def volume_access_path_change_client_group(self, ctxt, id, data):
        client_group_id = data.get("client_group_id")
        new_client_group_id = data.get("new_client_group_id")
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=["volume_gateways", "nodes"])
        client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, client_group_id, expected_attrs=["volume_clients"])
        new_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, new_client_group_id, expected_attrs=["volume_clients"])
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={"volume_access_path_id": access_path.id,
                           "volume_client_group_id": client_group_id})
        if not volume_mappings:
            logger.error("volume access path: %s, volume mapping not found",
                         access_path.name)
            raise exception.AccessPathNoMapping(access_path=access_path.name)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.ACCESS_PATH,
            action=AllActionType.ACCESS_PATH_UPDATE_CLIENT_GROUP,
            before_obj=access_path)
        volume_mapping_ids = [i.id for i in volume_mappings]
        logger.debug("access_path: %s, client_group_id: %s, "
                     "volume_mapping_ids: %s", access_path.name,
                     client_group_id, volume_mapping_ids)
        volume_ids = [i.volume_id for i in volume_mappings]
        volumes = []
        for volume_id in volume_ids:
            volume = objects.Volume.get_by_id(
                ctxt, volume_id, expected_attrs=["pool"])
            volumes.append(volume)
        self._volume_access_path_change_client_group(
            ctxt, access_path, volumes, client_group.volume_clients,
            new_client_group)
        for mapping_id in volume_mapping_ids:
            volume_mapping = objects.VolumeMapping.get_by_id(ctxt, mapping_id)
            volume_mapping.volume_client_group_id = new_client_group_id
            volume_mapping.save()
        access_path.updated_at = timeutils.utcnow()
        access_path.save()
        self.finish_action(begin_action, resource_id=access_path.id,
                           resource_name=access_path.name,
                           after_obj=access_path)
        return access_path

    def volume_access_path_get_mappings(self, ctxt, id):
        access_path_mappings = []
        expected_attrs = ['volume_client_groups']
        access_path = objects.VolumeAccessPath.get_by_id(
            ctxt, id, expected_attrs=expected_attrs)
        client_group_ids = [i.id for i in access_path.volume_client_groups]
        for client_group_id in client_group_ids:
            access_path_mapping = {}
            client_group = objects.VolumeClientGroup.get_by_id(
                ctxt, client_group_id)
            volumes = []
            vol_ids = []
            access_path.volume_client_groups = None
            access_path_mapping["access_path"] = access_path
            access_path_mapping["client_group"] = client_group
            volume_mappings = objects.VolumeMappingList.get_all(
                ctxt, filters={"volume_access_path_id": access_path.id,
                               "volume_client_group_id": client_group_id})
            volume_ids = [i.volume_id for i in volume_mappings]
            for vol_id in volume_ids:
                if vol_id not in vol_ids:
                    vol_ids.append(vol_id)
            logger.debug("client_group_id: %s, vol_id: %s", client_group_id,
                         vol_ids)
            for vol_id in vol_ids:
                volume = objects.Volume.get_by_id(ctxt, vol_id)
                volumes.append(volume)
            access_path_mapping["volumes"] = volumes
            logger.debug("access_path_mapping: %s", access_path_mapping)
            access_path_mappings.append(access_path_mapping)
        return access_path_mappings
