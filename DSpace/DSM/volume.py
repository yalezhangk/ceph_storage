import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.taskflows.ceph import CephTask

logger = logging.getLogger(__name__)


class VolumeHandler(AdminBaseHandler):
    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None,
                       expected_attrs=None):
        return objects.VolumeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def volume_get_count(self, ctxt, filters=None):
        return objects.VolumeList.get_count(ctxt, filters=filters)

    def volume_get(self, ctxt, volume_id, expected_attrs=None):
        volume = objects.Volume.get_by_id(ctxt, volume_id,
                                          expected_attrs=expected_attrs)
        if not volume:
            return volume
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        pool_name = pool.pool_name
        v_name = volume.volume_name
        try:
            ceph_client = CephTask(ctxt)
            used_size = ceph_client.rbd_used_size(pool_name, v_name)
        except Exception as e:
            used_size = None
            logger.exception('get volume: %s used_size error,%s', v_name,
                             str(e))
        volume.used = used_size
        return volume

    def _check_volume_size(self, ctxt, data):
        pool_id = data.get('pool_id')
        pool_size = self.pool_fact_total_size_bytes(ctxt, pool_id)
        size = data.get('size')
        if not pool_size.get('is_avaible'):
            raise exception.InvalidInput(
                reason=_('current pool size is fulled'))
        if size > pool_size.get('size', 0):
            raise exception.InvalidInput(
                reason=_('size can not over current pool_total_size'))

    def _check_name_exist(self, ctxt, data):
        display_name = data.get('display_name')
        is_exist = objects.VolumeList.get_all(
            ctxt, filters={'display_name': display_name})
        if is_exist:
            raise exception.InvalidInput(
                reason=_('Volume name {} already exists!').format(
                    display_name))

    def _check_volume_mappings(self, ctxt, volume_id):
        volume_mappings = objects.VolumeMappingList.get_all(
            ctxt, filters={'volume_id': volume_id})
        if volume_mappings:
            raise exception.InvalidInput(
                reason=_(
                    'Volume mapped to access path can not extend or shrink'))

    def volume_create(self, ctxt, data):
        self.check_mon_host(ctxt)
        self._check_volume_size(ctxt, data)
        self._check_name_exist(ctxt, data)
        batch_create = data.get('batch_create')
        if batch_create:
            # 批量创建
            logger.debug('begin batch_create volume')
            prefix_name = data.get('display_name')
            number = data.get('number')
            volumes = []
            for i in range(int(number)):
                volume_data = {
                    'volume_name': "volume-{}".format(str(uuid.uuid4())),
                    'size': data.get('size'),
                    'status': s_fields.VolumeStatus.CREATING,
                    'display_name': '{}-{}'.format(prefix_name, i + 1),
                    'display_description': data.get('display_description'),
                    'pool_id': data.get('pool_id'),
                    'cluster_id': ctxt.cluster_id
                }
                begin_action = self.begin_action(ctxt, AllResourceType.VOLUME,
                                                 AllActionType.CREATE)
                volume = objects.Volume(ctxt, **volume_data)
                volume.create()
                volume = objects.Volume.get_by_id(ctxt, volume.id,
                                                  joined_load=True)
                volumes.append(volume)
                self.task_submit(self._volume_create, ctxt, volume,
                                 begin_action)
                logger.info('volume create task has begin,volume_name=%s',
                            volume_data['display_name'])
            return volumes
        else:
            display_name = data.get('display_name')
            logger.debug('begin create volume, display_name: %s', display_name)
            volume_data = {
                'volume_name': "volume-{}".format(str(uuid.uuid4())),
                'size': data.get('size'),
                'status': s_fields.VolumeStatus.CREATING,
                'display_name': display_name,
                'display_description': data.get('display_description'),
                'pool_id': data.get('pool_id'),
                'cluster_id': ctxt.cluster_id
            }
            begin_action = self.begin_action(ctxt, AllResourceType.VOLUME,
                                             AllActionType.CREATE)
            volume = objects.Volume(ctxt, **volume_data)
            volume.create()
            volume = objects.Volume.get_by_id(ctxt, volume.id,
                                              joined_load=True)
            # put into thread pool
            self.task_submit(self._volume_create, ctxt, volume, begin_action)
            logger.info('volume create task has begin,volume_name=%s',
                        volume_data['display_name'])
            return volume

    def _volume_create(self, ctxt, volume, begin_action=None):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        size = volume.size
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_create(pool.pool_name, volume_name, size)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_create success,volume_name=%s', volume_name)
            op_status = "CREATE_SUCCESS"
            msg = _("create volume success: %s") % volume.display_name
            err_msg = None
        except exception.StorException as e:
            logger.exception('volume_create error,volume_name=%s,reason:%s',
                             volume_name, str(e))
            status = s_fields.VolumeStatus.ERROR
            op_status = "CREATE_ERROR"
            msg = _("create volume error: %s") % volume.display_name
            err_msg = str(e)
        volume.status = status
        volume.save()
        self.finish_action(begin_action, volume.id, volume.display_name,
                           volume, status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, volume, op_status, msg)

    def volume_update(self, ctxt, volume_id, data):
        logger.debug('volume begin update display_name')
        volume = self.volume_get(ctxt, volume_id)
        display_name = data.get('display_name')
        if volume.display_name == display_name:
            pass
        else:
            self._check_name_exist(ctxt, data)
        begin_action = self.begin_action(ctxt, AllResourceType.VOLUME,
                                         AllActionType.UPDATE, volume)
        volume.display_name = display_name
        volume.save()
        logger.info('volume update success,volume_name=%s', display_name)
        self.finish_action(begin_action, volume_id, display_name, volume)
        return volume

    def _check_volume_status(self, volume):
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()

    def _verify_volume_del(self, ctxt, volume):
        self.check_mon_host(ctxt)
        self._check_volume_status(volume)
        has_snap = objects.VolumeSnapshotList.get_all(ctxt, filters={
            'volume_id': volume.id})
        if has_snap:
            raise exception.InvalidInput(reason=_(
                'The volume {} has snapshot').format(volume.display_name))
        if volume.volume_access_path:
            raise exception.InvalidInput(
                reason=_('The volume {} has related access_path').format(
                    volume.display_name))

    def volume_delete(self, ctxt, volume_id):
        expected_attrs = ['volume_access_path']
        volume = objects.Volume.get_by_id(
            ctxt, volume_id, expected_attrs=expected_attrs)
        logger.debug('volume begin delete: %s', volume.display_name)
        self._verify_volume_del(ctxt, volume)
        begin_action = self.begin_action(
            ctxt, AllResourceType.VOLUME, AllActionType.DELETE, volume)
        volume.status = s_fields.VolumeStatus.DELETING
        volume.save()
        self.task_submit(self._volume_delete, ctxt, volume, begin_action)
        logger.info('volume delete task has begin,volume_name=%s',
                    volume.display_name)
        return volume

    def _volume_delete(self, ctxt, volume, begin_action):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_delete(pool.pool_name, volume_name)
            logger.info('volume_delete success,volume_name=%s', volume_name)
            msg = _("delete volume success: %s") % volume.display_name
            status = 'success'
            volume.destroy()
            op_status = "DELETE_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_delete error,volume_name=%s,reason:%s',
                         volume_name, str(e))
            msg = _("delete volume error: %s") % volume.display_name
            volume.status = status
            volume.save()
            op_status = "DELETE_ERROR"
            err_msg = str(e)
        self.finish_action(begin_action, volume.id, volume.display_name,
                           volume, status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, volume, op_status, msg)

    def volume_extend(self, ctxt, volume_id, data):
        # 扩容
        self._check_volume_mappings(ctxt, volume_id)
        volume = objects.Volume.get_by_id(ctxt, volume_id, joined_load=True)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        logger.debug('volume: %s begin extend', volume.display_name)
        self.check_mon_host(ctxt)
        self._check_volume_status(volume)
        data.update({'pool_id': volume.pool_id})
        self._check_volume_size(ctxt, data)
        begin_action = self.begin_action(
            ctxt, AllResourceType.VOLUME, AllActionType.VOLUME_EXTEND, volume)
        extra_data = {'old_size': volume.size}
        volume.size = data.get('size')
        volume.status = s_fields.VolumeStatus.EXTENDING
        volume.save()
        self.task_submit(self._volume_resize, ctxt, volume, extra_data,
                         begin_action)
        logger.info('volume extend task has begin,volume_name=%s',
                    volume.display_name)
        return volume

    def _volume_resize(self, ctxt, volume, extra_data, begin_action=None):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        size = volume.size
        old_size = extra_data.get('old_size')
        if size > old_size:
            op_action = 'VOLUME_EXTEND'
            op_msg = _('volume extend')
        else:
            op_action = 'VOLUME_SHRINK'
            op_msg = _('volume shrink')
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_resize(pool.pool_name, volume_name, size)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_resize success,volume_name=%s, size=%s',
                        volume_name, size)
            op_status = '{}_SUCCESS'.format(op_action)
            now_size = size
            msg = _('{} success: {}').format(op_msg, volume.display_name)
            err_msg = None
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_resize error,volume_name=%s,reason:%s',
                         volume_name, str(e))
            op_status = '{}_ERROR'.format(op_action)
            now_size = old_size
            msg = _('{} error: {}').format(op_msg, volume.display_name)
            err_msg = str(e)
        volume.status = status
        volume.size = now_size
        volume.save()
        self.finish_action(begin_action, volume.id, volume.display_name,
                           volume, status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, volume, op_status, msg)

    def volume_shrink(self, ctxt, volume_id, data):
        # 缩容
        self._check_volume_mappings(ctxt, volume_id)
        volume = objects.Volume.get_by_id(ctxt, volume_id, joined_load=True)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        logger.debug('volume: %s begin shrink', volume.display_name)
        self.check_mon_host(ctxt)
        self._check_volume_status(volume)
        data.update({'pool_id': volume.pool_id})
        # size can not more than pool size
        self._check_volume_size(ctxt, data)
        begin_action = self.begin_action(
            ctxt, AllResourceType.VOLUME, AllActionType.VOLUME_SHRINK, volume)
        extra_data = {'old_size': volume.size}
        volume.size = data.get('size')
        volume.status = s_fields.VolumeStatus.SHRINKING
        volume.save()
        self.task_submit(self._volume_resize, ctxt, volume, extra_data,
                         begin_action)
        logger.info('volume shrink task has begin,volume_name=%s',
                    volume.display_name)
        return volume

    def volume_rollback(self, ctxt, volume_id, data):
        expected_attrs = ['volume_access_path']
        volume = objects.Volume.get_by_id(ctxt, volume_id,
                                          expected_attrs=expected_attrs)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        logger.debug('volume: %s begin rollback', volume.display_name)
        self.check_mon_host(ctxt)
        self._check_volume_status(volume)
        snap_id = data.get('volume_snapshot_id')
        snap = objects.VolumeSnapshot.get_by_id(ctxt, snap_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(volume_snapshot_id=snap_id)
        if snap.volume_id != int(volume_id):
            raise exception.InvalidInput(_(
                'The volume: {} has not the snap: {}').format(
                volume.display_name, snap.display_name))
        if volume.volume_access_path:
            raise exception.InvalidInput(_(
                'The volume: {} has access_path, can not rollback').format(
                volume.display_name))
        begin_action = self.begin_action(
            ctxt, AllResourceType.VOLUME, AllActionType.VOLUME_ROLLBACK,
            volume)
        volume.status = s_fields.VolumeStatus.ROLLBACKING
        volume.save()
        extra_data = {'snap_name': snap.uuid}
        self.task_submit(self._volume_rollback, ctxt, volume, extra_data,
                         begin_action)
        logger.info('volume rollback task has begin,volume_name=%s',
                    volume.display_name)
        return volume

    def _volume_rollback(self, ctxt, volume, extra_data, begin_action=None):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        snap_name = extra_data.get('snap_name')
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_rollback_to_snap(pool.pool_name, volume_name,
                                             snap_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('vulume_rollback success,%s@%s',
                        volume_name, snap_name)
            op_status = "VOLUME_ROLLBACK_SUCCESS"
            msg = _("volume rollback success: %s") % volume.display_name
            err_msg = None
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_rollback error,%s@%s,reason:%s',
                         volume_name, snap_name, str(e))
            op_status = "VOLUME_ROLLBACK_ERROR"
            msg = _("volume rollback error: %s") % volume.display_name
            err_msg = str(e)
        volume.status = status
        volume.save()
        self.finish_action(begin_action, volume.id, volume.display_name,
                           volume, status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, volume, op_status, msg)

    def volume_unlink(self, ctxt, volume_id):
        volume = objects.Volume.get_by_id(ctxt, volume_id, joined_load=True)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        logger.debug('begin volume_unlink: %s', volume.display_name)
        self.check_mon_host(ctxt)
        self._check_volume_status(volume)
        if not volume.is_link_clone:
            raise exception.Invalid(
                msg=_('the volume_name {} has not relate_snap').format(
                    volume.display_name))
        begin_action = self.begin_action(
            ctxt, AllResourceType.VOLUME, AllActionType.VOLUME_UNLINK, volume)
        volume.status = s_fields.VolumeStatus.UNLINKING
        volume.save()
        self.task_submit(self._volume_unlink, ctxt, volume, begin_action)
        logger.info('volume unlink task has begin,volume_name=%s',
                    volume.display_name)
        return volume

    def _volume_unlink(self, ctxt, volume, begin_action=None):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        if not pool:
            raise exception.PoolNotFound(pool_id=volume.pool_id)
        pool_name = pool.pool_name
        volume_name = volume.volume_name
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_flatten(pool_name, volume_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_unlink success,%s/%s', pool_name, volume_name)
            op_status = "VOLUME_UNLINK_SUCCESS"
            msg = _("volume unlink success: %s") % volume.display_name
            err_msg = None
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_unlink error,%s/%s,reason:%s',
                         pool_name, volume_name, str(e))
            op_status = "VOLUME_UNLINK_ERROR"
            msg = _("volume unlink error: %s") % volume.display_name
            err_msg = str(e)
        if status == s_fields.VolumeStatus.ACTIVE:
            volume.is_link_clone = False  # 断开关系链
        volume.status = status
        volume.save()
        self.finish_action(begin_action, volume.id, volume.display_name,
                           volume, status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, volume, op_status, msg)

    def _volume_create_from_snapshot(self, ctxt, verify_data, begin_action):
        p_pool_name = verify_data['p_pool_name']
        p_volume_name = verify_data['p_volume_name']
        p_snap_name = verify_data['p_snap_name']
        c_pool_name = verify_data['c_pool_name']
        new_volume = verify_data['new_volume']
        c_volume_name = new_volume.volume_name
        is_link_clone = verify_data['is_link_clone']
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_clone_volume(
                p_pool_name, p_volume_name, p_snap_name, c_pool_name,
                c_volume_name)
            if not is_link_clone:  # 独立克隆，断开关系链
                ceph_client.rbd_flatten(c_pool_name, c_volume_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume clone success, volume_name=%s', c_volume_name)
            op_status = "VOLUME_CLONE_SUCCESS"
            msg = _('volume clone success: %s') % new_volume.display_name
            err_msg = None
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume clone error, volume_name=%s,reason:%s',
                         c_volume_name, str(e))
            op_status = "VOLUME_CLONE_ERROR"
            msg = _('volume clone error: %s') % new_volume.display_name
            err_msg = str(e)
        new_volume.status = status
        new_volume.save()
        self.finish_action(begin_action, new_volume.id,
                           new_volume.display_name,
                           new_volume, status, err_msg=err_msg)
        # send msg
        self.send_websocket(ctxt, new_volume, op_status, msg)

    def _verify_clone_data(self, ctxt, snapshot_id, data):
        self.check_mon_host(ctxt)
        display_name = data.get('display_name')
        is_exist = objects.VolumeList.get_all(
            ctxt, filters={'display_name': display_name})
        if is_exist:
            raise exception.InvalidInput(
                reason=_('Volume name {} already exists!').format(
                    display_name))
        display_description = data.get('display_description')
        c_pool_id = data.get('pool_id')
        is_link_clone = data.get('is_link_clone')
        snap = objects.VolumeSnapshot.get_by_id(ctxt, snapshot_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=snapshot_id)
        snap_name = snap.uuid
        p_volume = objects.Volume.get_by_id(ctxt, snap.volume_id)
        if not p_volume:
            raise exception.VolumeNotFound(volume_id=snap.volume_id)
        size = p_volume.size
        p_volume_name = p_volume.volume_name
        p_pool_name = objects.Pool.get_by_id(ctxt, p_volume.pool_id).pool_name
        c_pool_name = objects.Pool.get_by_id(ctxt, c_pool_id).pool_name
        return {
            'p_pool_name': p_pool_name,
            'p_volume_name': p_volume_name,
            'p_snap_name': snap_name,
            'c_pool_name': c_pool_name,
            'pool_id': c_pool_id,
            'display_name': display_name,
            'size': size,
            'display_description': display_description,
            'is_link_clone': is_link_clone,
        }

    def volume_create_from_snapshot(self, ctxt, snapshot_id, data):
        logger.debug('begin volume_create_from_snapshot')
        verify_data = self._verify_clone_data(ctxt, snapshot_id, data)
        # create volume
        batch_create = data.get('batch_create')
        if batch_create:
            # 批量克隆
            number = data.get('number')
            prefix_name = verify_data['display_name']
            logger.debug('begin batch_clone, prefix_name=%s', prefix_name)
            new_volumes = []
            for i in range(int(number)):
                volume_data = {
                    "cluster_id": ctxt.cluster_id,
                    'volume_name': F'volume-{str(uuid.uuid4())}',
                    'display_name': F'{prefix_name}-{i+1}',
                    'display_description': verify_data['display_description'],
                    'size': verify_data['size'],
                    'status': s_fields.VolumeStatus.CREATING,
                    'snapshot_id': snapshot_id,
                    'is_link_clone': verify_data['is_link_clone'],
                    'pool_id': verify_data['pool_id']
                }
                begin_action = self.begin_action(ctxt, AllResourceType.VOLUME,
                                                 AllActionType.CLONE)
                new_volume = objects.Volume(ctxt, **volume_data)
                new_volume.create()
                new_volume = objects.Volume.get_by_id(ctxt, new_volume.id,
                                                      joined_load=True)
                verify_data.update({'new_volume': new_volume})
                # put into thread pool
                self.task_submit(self._volume_create_from_snapshot, ctxt,
                                 verify_data, begin_action)
                logger.info('volume clone task has begin,volume_name=%s',
                            volume_data['display_name'])
                new_volumes.append(new_volume)
            return new_volumes
        else:
            display_name = verify_data['display_name']
            logger.debug('begin clone, volume_name=%s', display_name)
            begin_action = self.begin_action(ctxt, AllResourceType.VOLUME,
                                             AllActionType.CLONE)
            uid = str(uuid.uuid4())
            volume_name = "volume-{}".format(uid)
            volume_data = {
                "cluster_id": ctxt.cluster_id,
                'volume_name': volume_name,
                'display_name': display_name,
                'display_description': verify_data['display_description'],
                'size': verify_data['size'],
                'status': s_fields.VolumeStatus.CREATING,
                'snapshot_id': snapshot_id,
                'is_link_clone': verify_data['is_link_clone'],
                'pool_id': verify_data['pool_id']
            }
            new_volume = objects.Volume(ctxt, **volume_data)
            new_volume.create()
            new_volume = objects.Volume.get_by_id(ctxt, new_volume.id,
                                                  joined_load=True)
            verify_data.update({'new_volume': new_volume})
            # put into thread pool
            self.task_submit(self._volume_create_from_snapshot, ctxt,
                             verify_data, begin_action)
            logger.info('volume clone task has begin,volume_name=%s',
                        volume_data['display_name'])
            return new_volume
