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
from t2stor.taskflows.node import NodeTask

logger = logging.getLogger(__name__)


class OsdHandler(AdminBaseHandler):

    def osd_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None,
                    expected_attrs=None):
        filters = filters or {}
        return objects.OsdList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def osd_get(self, ctxt, osd_id, expected_attrs=None):
        return objects.Osd.get_by_id(ctxt, osd_id,
                                     expected_attrs=expected_attrs)

    def _osd_create(self, ctxt, node, osd):
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_install(osd)
            msg = _("Osd created!")
            osd.status = s_fields.OsdStatus.UP
            osd.save()
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            msg = _("Osd create error!")

        wb_client = WebSocketClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, osd, "CREATED", msg)

    def _set_osd_partation_role(self, osd):
        osd.disk.status = s_fields.DiskStatus.INUSE
        osd.disk.save()
        if osd.db_partition_id:
            osd.db_partition.role = s_fields.DiskPartitionRole.DB
            osd.db_partition.status = s_fields.DiskStatus.INUSE
            osd.db_partition.save()
        if osd.cache_partition_id:
            osd.cache_partition.role = s_fields.DiskPartitionRole.CACHE
            osd.cache_partition.status = s_fields.DiskStatus.INUSE
            osd.cache_partition.save()
        if osd.journal_partition_id:
            role = s_fields.DiskPartitionRole.JOURNAL
            osd.journal_partition.role = role
            osd.journal_partition.status = s_fields.DiskStatus.INUSE
            osd.journal_partition.save()
        if osd.wal_partition_id:
            osd.wal_partition.role = s_fields.DiskPartitionRole.WAL
            osd.wal_partition.status = s_fields.DiskStatus.INUSE
            osd.wal_partition.save()

    def _osd_get_free_id(self, ctxt, osd_fsid):
        task = CephTask(ctxt)
        return task.osd_new(osd_fsid)

    def _osd_config_set(self, ctxt, osd):
        if osd.cache_partition_id:
            ceph_cfg = objects.CephConfig(
                ctxt, group="osd.%s" % osd.osd_id,
                key='backend_type',
                value='t2ce'
            )
            ceph_cfg.create()

    def osd_create(self, ctxt, data):
        node_id = data.get('node_id')
        osd_fsid = str(uuid.uuid4())
        node = objects.Node.get_by_id(ctxt, node_id)
        osd = objects.Osd(
            ctxt, node_id=node_id,
            fsid=osd_fsid,
            osd_id=self._osd_get_free_id(ctxt, osd_fsid),
            type=data.get('type'),
            db_partition_id=data.get('db_partition_id'),
            wal_partition_id=data.get('wal_partition_id'),
            cache_partition_id=data.get('cache_partition_id'),
            journal_partition_id=data.get('journal_partition_id'),
            disk_id=data.get('disk_id'),
            disk_type="ssd",
            status=s_fields.OsdStatus.CREATING
        )
        osd.create()
        osd = objects.Osd.get_by_id(ctxt, osd.id, joined_load=True)
        self._set_osd_partation_role(osd)
        self._osd_config_set(ctxt, osd)

        self.executor.submit(self._osd_create, ctxt, node, osd)

        return osd

    def osd_update(self, ctxt, osd_id, data):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        for k, v in six.iteritems(data):
            setattr(osd, k, v)
        osd.save()
        return osd

    def _osd_config_remove(self, ctxt, osd):
        logger.debug("osd clear config")
        if osd.cache_partition_id:
            osd_cfgs = objects.CephConfigList.get_all(
                ctxt, filters={'group': "osd.%s" % osd.osd_id}
            )
            for cfg in osd_cfgs:
                cfg.destroy()

    def _osd_clear_partition_role(self, osd):
        logger.debug("osd clear partition role")
        osd.disk.status = s_fields.DiskStatus.AVAILABLE
        osd.disk.save()
        if osd.db_partition_id:
            osd.db_partition.role = None
            osd.db_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.db_partition.save()
        if osd.cache_partition_id:
            osd.cache_partition.role = None
            osd.cache_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.cache_partition.save()
        if osd.journal_partition_id:
            osd.journal_partition.role = None
            osd.journal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.journal_partition.save()
        if osd.wal_partition_id:
            osd.wal_partition.role = None
            osd.wal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.wal_partition.save()

    def _osd_delete(self, ctxt, node, osd):
        osd_id = osd.id
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_uninstall(osd)
            self._osd_config_remove(ctxt, osd)
            self._osd_clear_partition_role(osd)
            osd.destroy()
            msg = _("Osd uninstall!")
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            msg = _("Osd create error!")

        osd = objects.Osd.get_by_id(ctxt, osd_id)
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, osd, "DELETED", msg)

    def osd_delete(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        osd.status = s_fields.OsdStatus.DELETING
        osd.save()
        self.executor.submit(self._osd_delete, ctxt, osd.node, osd)
        return osd
