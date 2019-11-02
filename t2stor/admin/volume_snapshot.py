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

logger = logging.getLogger(__name__)


class VolumeSnapshotHandler(AdminBaseHandler):
    def volume_snapshot_get_all(self, ctxt, marker=None, limit=None,
                                sort_keys=None, sort_dirs=None, filters=None,
                                offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.VolumeSnapshotList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_snapshot_create(self, ctxt, data):
        volume_id = data.get('volume_id')
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        data.update({'volume_name': volume.volume_name,
                     'pool_name': pool.pool_name})
        uid = str(uuid.uuid4())
        data.update({
            'cluster_id': ctxt.cluster_id,
            'uuid': uid,
            'status': s_fields.VolumeSnapshotStatus.CREATING
        })
        extra_data = {'volume_name': data.pop('volume_name'),
                      'pool_name': data.pop('pool_name')}
        snap = objects.VolumeSnapshot(ctxt, **data)
        snap.create()
        # TODO create snapshot
        self.executor.submit(self._snap_create, ctxt, snap, extra_data)
        return snap

    def _snap_create(self, ctxt, snap, extra_data):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_snap_create(pool_name, volume_name, snap_name)
            # 新创建的快照均开启快照保护
            ceph_client.rbd_protect_snap(pool_name, volume_name, snap_name)
            status = s_fields.VolumeSnapshotStatus.ACTIVE
            logger.info('create snapshot success,{}/{}@{}'.format(
                pool_name, volume_name, snap_name))
            msg = 'volume_snapshot create success'
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            logger.error('create snapshot error,{}/{}@{},reason:{}'.format(
                pool_name, volume_name, snap_name, str(e)))
            msg = 'volume_snapshot create error'
        snap.status = status
        snap.save()
        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=snap.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, snap, "CREATED", msg)

    def volume_snapshot_get(self, ctxt, volume_snapshot_id):
        return objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id)

    def volume_snapshot_update(self, ctxt, volume_snapshot_id, data):
        volume_snapshot = self.volume_snapshot_get(ctxt, volume_snapshot_id)
        for k, v in six.iteritems(data):
            setattr(volume_snapshot, k, v)
        volume_snapshot.save()
        return volume_snapshot

    def _snap_delete(self, ctxt, snap, extra_data):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            # 关闭快照保护，再删除快照
            ceph_client.rbd_unprotect_snap(pool_name, volume_name, snap_name)
            ceph_client.rbd_snap_delete(pool_name, volume_name, snap_name)
            snap.destroy()
            logger.info('snapshot_delete success,snap_name={}'.format(
                snap_name))
            msg = _("delete snapshot success")
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            snap.status = status
            snap.save()
            logger.error('snapshot_delete error,{}/{}@{},reason:{}'.format(
                pool_name, volume_name, snap_name, str(e)))
            msg = _('delete snapshot error')
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=snap.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, snap, 'DELETED', msg)

    def volume_snapshot_delete(self, ctxt, volume_snapshot_id):
        snap = objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=volume_snapshot_id)
        volume = objects.Volume.get_by_id(ctxt, snap.volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=snap.volume_id)
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        snap_data = {'volume_name': volume.volume_name,
                     'pool_name': pool.pool_name,
                     'snap': snap}
        snap = snap_data['snap']
        snap.status = s_fields.VolumeSnapshotStatus.DELETING
        snap.save()
        self.executor.submit(self._snap_delete, ctxt, snap, snap_data)
        return snap
