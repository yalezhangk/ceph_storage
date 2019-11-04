import uuid

import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask

logger = logging.getLogger(__name__)


class VolumeHandler(AdminBaseHandler):
    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None,
                       expected_attrs=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.VolumeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def volume_get(self, ctxt, volume_id, expected_attrs=None):
        return objects.Volume.get_by_id(ctxt, volume_id,
                                        expected_attrs=expected_attrs)

    def volume_create(self, ctxt, data):
        batch_create = data.get('batch_create')
        if batch_create:
            # 批量创建
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
                volume = objects.Volume(ctxt, **volume_data)
                volume.create()
                volumes.append(volume)
                self.executor.submit(self._volume_create, ctxt, volume)
            return volumes
        else:
            volume_data = {
                'volume_name': "volume-{}".format(str(uuid.uuid4())),
                'size': data.get('size'),
                'status': s_fields.VolumeStatus.CREATING,
                'display_name': data.get('display_name'),
                'display_description': data.get('display_description'),
                'pool_id': data.get('pool_id'),
                'cluster_id': ctxt.cluster_id
            }
            volume = objects.Volume(ctxt, **volume_data)
            volume.create()
            # put into thread pool
            self.executor.submit(self._volume_create, ctxt, volume)
            return volume

    def _volume_create(self, ctxt, volume):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        size = volume.size
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_create(pool.pool_name, volume_name, size)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_create success,volume_name={}'.format(
                volume_name))
            msg = _("create volume success")
        except exception.StorException as e:
            logger.error('volume_create error,volume_name={},reason:{}'.format(
                volume, str(e)))
            status = s_fields.VolumeStatus.ERROR
            msg = _("create volume error")
        volume.status = status
        volume.save()
        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=volume.cluster_id).get_client()
        wb_client.send_message(ctxt, volume, "CREATED", msg)

    def volume_update(self, ctxt, volume_id, data):
        volume = self.volume_get(ctxt, volume_id)
        for k, v in six.iteritems(data):
            setattr(volume, k, v)
        volume.save()
        return volume

    def volume_delete(self, ctxt, volume_id):
        volume = self.volume_get(ctxt, volume_id)
        volume.status = s_fields.VolumeStatus.DELETING
        volume.save()
        self.executor.submit(self._volume_delete, ctxt, volume)
        return volume

    def _volume_delete(self, ctxt, volume):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_delete(pool.pool_name, volume_name)
            logger.info('volume_delete success,volume_name={}'.format(
                volume_name))
            msg = _("delete volume success")
            volume.destroy()
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_delete error,volume_name={},reason:{}'.format(
                volume_name, str(e)))
            msg = _("delete volume error")
            volume.status = status
            volume.save()
        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt,
            cluster_id=volume.cluster_id).get_client()
        wb_client.send_message(ctxt, volume, 'DELETED', msg)

    def volume_extend(self, ctxt, volume_id, data):
        # 扩容
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()
        extra_data = {'old_size': volume.size}
        for k, v in six.iteritems(data):
            setattr(volume, k, v)
        volume.save()
        self.executor.submit(self._volume_resize, ctxt, volume, extra_data)
        return volume

    def _volume_resize(self, ctxt, volume, extra_data):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        size = volume.size
        old_size = extra_data.get('old_size')
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_resize(pool.pool_name, volume_name, size)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_resize success,volume_name={}, size={}'
                        .format(volume_name, size))
            now_size = size
            msg = _("volume resize success")
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_resize error,volume_name={},reason:{}'
                         .format(volume_name, str(e)))
            now_size = old_size
            msg = _("volume resize error")
        volume.status = status
        volume.size = now_size
        volume.save()
        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt,
            cluster_id=volume.cluster_id).get_client()
        wb_client.send_message(ctxt, volume, 'VOLUME_RESIZE', msg)

    def volume_shrink(self, ctxt, volume_id, data):
        # 缩容
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()
        extra_data = {'old_size': volume.size}
        for k, v in six.iteritems(data):
            setattr(volume, k, v)
        volume.save()
        self.executor.submit(self._volume_resize, ctxt, volume, extra_data)
        return volume

    def volume_rollback(self, ctxt, volume_id, data):
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()
        snap_id = data.get('volume_snapshot_id')
        snap = objects.VolumeSnapshot.get_by_id(ctxt, snap_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(volume_snapshot_id=snap_id)
        if snap.volume_id != int(volume_id):
            raise exception.InvalidInput(_(
                'The volume_id {} has not the snap_id {}').format(
                volume_id, snap_id))
        # todo other verify
        data.update({'snap_name': snap.uuid})
        extra_data = {'snap_name': data.get('snap_name')}
        self.executor.submit(self._volume_rollback, ctxt, volume, extra_data)
        return volume

    def _volume_rollback(self, ctxt, volume, extra_data):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        volume_name = volume.volume_name
        snap_name = extra_data.get('snap_name')
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_rollback_to_snap(pool.pool_name, volume_name,
                                             snap_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('vulume_rollback success,{}@{}'.format(
                volume_name, snap_name))
            msg = _("volume rollback success")
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_rollback error,{}@{},reason:{}'.format(
                volume_name, snap_name, str(e)))
            msg = _("volume rollback error")
        volume.status = status
        volume.save()
        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt,
            cluster_id=volume.cluster_id).get_client()
        wb_client.send_message(ctxt, volume, 'VOLUME_ROLLBACK', msg)

    def volume_unlink(self, ctxt, volume_id):
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()
        if not volume.is_link_clone:
            raise exception.Invalid(
                msg=_('the volume_name {} has not relate_snap').format(
                    volume.volume_name))
        self.executor.submit(self._volume_unlink, ctxt, volume)
        return volume

    def _volume_unlink(self, ctxt, volume):
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        if not pool:
            raise exception.PoolNotFound(pool_id=volume.pool_id)
        pool_name = pool.pool_name
        volume_name = volume.volume_name
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_flatten(pool_name, volume_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume_unlink success,{}/{}'.format(
                pool_name, volume_name))
            msg = _("volume unlink success")
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume_unlink error,{}/{},reason:{}'
                         .format(pool_name, volume_name, str(e)))
            msg = _("volume unlink error")
        if status == s_fields.VolumeStatus.ACTIVE:
            volume.is_link_clone = False  # 断开关系链
        volume.status = status
        volume.save()
        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt,
            cluster_id=volume.cluster_id).get_client()
        wb_client.send_message(ctxt, volume, 'VOLUME_UNLINK', msg)

    def _volume_create_from_snapshot(self, ctxt, verify_data):
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
            logger.info('volume clone success, volume_name={}'.format(
                c_volume_name))
            msg = 'volume clone success'
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume clone error, volume_name={},reason:{}'.format(
                c_volume_name, str(e)))
            msg = 'volume clone error'
        new_volume.status = status
        new_volume.save()
        # send msg
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=new_volume.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, new_volume, 'VOLUME_CLONE', msg)

    def _verify_clone_data(self, ctxt, snapshot_id, data):
        display_name = data.get('display_name')
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
        verify_data = self._verify_clone_data(ctxt, snapshot_id, data)
        # create volume
        batch_create = data.get('batch_create')
        if batch_create:
            # 批量创建
            number = data.get('number')
            prefix_name = verify_data['display_name']
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
                new_volume = objects.Volume(ctxt, **volume_data)
                new_volume.create()
                verify_data.update({'new_volume': new_volume})
                # put into thread pool
                self.executor.submit(self._volume_create_from_snapshot, ctxt,
                                     verify_data)
                new_volumes.append(new_volume)
            return new_volumes
        else:
            uid = str(uuid.uuid4())
            volume_name = "volume-{}".format(uid)
            volume_data = {
                "cluster_id": ctxt.cluster_id,
                'volume_name': volume_name,
                'display_name': verify_data['display_name'],
                'display_description': verify_data['display_description'],
                'size': verify_data['size'],
                'status': s_fields.VolumeStatus.CREATING,
                'snapshot_id': snapshot_id,
                'is_link_clone': verify_data['is_link_clone'],
                'pool_id': verify_data['pool_id']
            }
            new_volume = objects.Volume(ctxt, **volume_data)
            new_volume.create()
            verify_data.update({'new_volume': new_volume})
            # put into thread pool
            self.executor.submit(self._volume_create_from_snapshot, ctxt,
                                 verify_data)
            return new_volume