import uuid

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.base import AdminBaseHandler
from t2stor.api.wsclient import WebSocketClientManager
from t2stor.i18n import _
from t2stor.objects import fields as s_fields
from t2stor.taskflows.ceph import CephTask

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
OSD_ID_MAX = 1024 ^ 2
logger = logging.getLogger(__name__)
LOCAL_LOGFILE_DIR = '/var/log/t2stor_log/'


class VolumeHandler(AdminBaseHandler):
    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.VolumeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_get(self, ctxt, volume_id):
        return objects.Volume.get_by_id(ctxt, volume_id)

    def volume_create(self, ctxt, data):
        uid = str(uuid.uuid4())
        volume_name = "volume-{}".format(uid)
        data.update({
            'cluster_id': ctxt.cluster_id,
            'status': s_fields.VolumeStatus.CREATING,
            'volume_name': volume_name,
        })
        volume = objects.Volume(ctxt, **data)
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
