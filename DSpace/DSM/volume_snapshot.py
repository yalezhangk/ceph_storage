import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.taskflows.ceph import CephTask

logger = logging.getLogger(__name__)


class VolumeSnapshotHandler(AdminBaseHandler):

    def volume_snapshot_get_all(self, ctxt, marker=None, limit=None,
                                sort_keys=None, sort_dirs=None, filters=None,
                                offset=None, expected_attrs=None):
        return objects.VolumeSnapshotList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def volume_snapshot_get_count(self, ctxt, filters=None):
        return objects.VolumeSnapshotList.get_count(ctxt, filters=filters)

    def _check_snap_create(self, ctxt, data):
        self.check_mon_host(ctxt)
        display_name = data.get('display_name')
        is_exist = objects.VolumeSnapshotList.get_all(
            ctxt, filters={'display_name': display_name})
        if is_exist:
            raise exception.InvalidInput(
                reason=_('snap name {} already exists!').format(
                    display_name))
        volume_id = data.get('volume_id')
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        pool_id = volume.pool_id
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        if not pool:
            raise exception.PoolNotFound(pool_id=pool_id)
        snap_data = {
            'cluster_id': ctxt.cluster_id,
            'uuid': str(uuid.uuid4()),
            'display_name': display_name,
            'status': s_fields.VolumeSnapshotStatus.CREATING,
            'display_description': data.get('display_description'),
            'volume_id': volume_id,
            'volume_name': volume.volume_name,
            'pool_name': pool.pool_name
        }
        return snap_data

    def volume_snapshot_create(self, ctxt, data):
        snap_data = self._check_snap_create(ctxt, data)
        begin_action = self.begin_action(ctxt, AllResourceType.SNAPSHOT,
                                         AllActionType.CREATE)
        extra_data = {'volume_name': snap_data.pop('volume_name'),
                      'pool_name': snap_data.pop('pool_name')}
        snap = objects.VolumeSnapshot(ctxt, **snap_data)
        snap.create()
        self.task_submit(self._snap_create, ctxt, snap, extra_data,
                         begin_action)
        logger.info('snap create task has begin,snap_name=%s',
                    snap_data['display_name'])
        return snap

    def _snap_create(self, ctxt, snap, extra_data, begin_action):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_snap_create(pool_name, volume_name, snap_name)
            # 新创建的快照均开启快照保护
            ceph_client.rbd_protect_snap(pool_name, volume_name, snap_name)
            status = s_fields.VolumeSnapshotStatus.ACTIVE
            logger.info('create snapshot success,%s/%s@%s',
                        pool_name, volume_name, snap_name)
            msg = 'volume_snapshot create success'
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            logger.error('create snapshot error,%s/%s@%s,reason:%s',
                         pool_name, volume_name, snap_name, str(e))
            msg = 'volume_snapshot create error'
        snap.status = status
        snap.save()
        self.finish_action(begin_action, snap.id, snap.display_name,
                           objects.json_encode(snap), status)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, snap, "CREATED", msg)

    def volume_snapshot_get(self, ctxt, volume_snapshot_id,
                            expected_attrs=None):
        return objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id,
                                                expected_attrs=expected_attrs)

    def volume_snapshot_update(self, ctxt, volume_snapshot_id, data):
        volume_snapshot = self.volume_snapshot_get(ctxt, volume_snapshot_id)
        begin_action = self.begin_action(ctxt, AllResourceType.SNAPSHOT,
                                         AllActionType.UPDATE)
        display_name = data.get('display_name')
        display_description = data.get('display_description')
        volume_snapshot.display_name = display_name
        volume_snapshot.display_description = display_description
        volume_snapshot.save()
        logger.info('snapshot update success,snapshot_name=%s', display_name)
        self.finish_action(begin_action, volume_snapshot_id, display_name,
                           objects.json_encode(volume_snapshot))
        return volume_snapshot

    def _snap_delete(self, ctxt, snap, extra_data, begin_action):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            # 关闭快照保护，再删除快照
            ceph_client.rbd_unprotect_snap(pool_name, volume_name, snap_name)
            ceph_client.rbd_snap_delete(pool_name, volume_name, snap_name)
            snap.destroy()
            status = 'success'
            logger.info('snapshot_delete success,snap_name=%s', snap_name)
            msg = _("delete snapshot success")
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            snap.status = status
            snap.save()
            logger.error('snapshot_delete error,%s/%s@%s,reason:%s',
                         pool_name, volume_name, snap_name, str(e))
            msg = _('delete snapshot error')
        self.finish_action(begin_action, snap.id, snap.display_name,
                           objects.json_encode(snap), status)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, snap, 'DELETED', msg)

    def _check_del_snap(self, ctxt, volume_snapshot_id):
        self.check_mon_host(ctxt)
        expected_attrs = ['child_volumes']
        snap = objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id,
                                                expected_attrs=expected_attrs)
        if not snap:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=volume_snapshot_id)
        volume_id = snap.volume_id
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        pool_id = volume.pool_id
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        if not volume:
            raise exception.PoolNotFound(pool_id=pool_id)
        if snap.child_volumes:
            raise exception.InvalidInput(_(
                'snapshot {} has clone volume, can not del'
            ).format(snap.display_name))
        return {
            'volume_name': volume.volume_name,
            'pool_name': pool.pool_name,
            'snap': snap
        }

    def volume_snapshot_delete(self, ctxt, volume_snapshot_id):
        extra_data = self._check_del_snap(ctxt, volume_snapshot_id)
        begin_action = self.begin_action(
            ctxt, AllResourceType.SNAPSHOT, AllActionType.DELETE)
        snap = extra_data['snap']
        snap.status = s_fields.VolumeSnapshotStatus.DELETING
        snap.save()
        self.task_submit(self._snap_delete, ctxt, snap, extra_data,
                         begin_action)
        logger.info('snap delete task has begin,snap_name=%s',
                    snap.display_name)
        return snap
