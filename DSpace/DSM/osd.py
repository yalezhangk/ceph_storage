import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class OsdHandler(AdminBaseHandler):

    def osd_get_all(self, ctxt, tab=None, marker=None, limit=None,
                    sort_keys=None, sort_dirs=None, filters=None, offset=None,
                    expected_attrs=None):
        osds = objects.OsdList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

        if tab == "default":
            logger.debug("Get osd metrics: tab=default")
            ceph_client = CephTask(ctxt)
            capacity = ceph_client.get_osd_df()
            mapping = {}
            if capacity:
                osd_capacity = capacity.get('nodes') + capacity.get('stray')
                for c in osd_capacity:
                    mapping[str(c['id'])] = c
            for osd in osds:
                size = mapping.get(osd.osd_id)
                osd.metrics = {}
                if size:
                    osd.metrics.update({'kb': [0, size['kb']]})
                    osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
                    osd.metrics.update({'kb_used': [0, size['kb_used']]})
                prometheus = PrometheusTool(ctxt)
                prometheus.osd_get_pg_state(osd)

        if tab == "io":
            logger.debug("Get osd metrics: tab=io")
            prometheus = PrometheusTool(ctxt)
            for osd in osds:
                osd.metrics = {}
                prometheus.osd_disk_perf(osd)

        return osds

    def osd_get_count(self, ctxt, filters=None):
        return objects.OsdList.get_count(
            ctxt, filters=filters)

    def osd_get(self, ctxt, osd_id, expected_attrs=None):
        return objects.Osd.get_by_id(ctxt, osd_id,
                                     expected_attrs=expected_attrs)

    def _osd_create(self, ctxt, node, osd):
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_install(osd)
            msg = _("Osd created!")
            osd.status = s_fields.OsdStatus.AVAILABLE
            osd.save()
            logger.info("Osd %s create success.", osd.osd_id)
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            msg = _("Osd create error!")
            logger.info("Osd %s create error.", osd.osd_id)

        wb_client = WebSocketClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, osd, "CREATED", msg)

    def _set_osd_partation_role(self, osd):
        osd.disk.status = s_fields.DiskStatus.INUSE
        osd.disk.save()
        if osd.db_partition_id:
            osd.db_partition.status = s_fields.DiskStatus.INUSE
            osd.db_partition.save()
        if osd.cache_partition_id:
            osd.cache_partition.status = s_fields.DiskStatus.INUSE
            osd.cache_partition.save()
        if osd.journal_partition_id:
            osd.journal_partition.status = s_fields.DiskStatus.INUSE
            osd.journal_partition.save()
        if osd.wal_partition_id:
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

    def _osd_create_disk_check(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if disk.status != s_fields.DiskStatus.AVAILABLE:
            raise exception.InvalidInput(_("Disk %s not available") % disk_id)
        if disk.role != s_fields.DiskRole.DATA:
            raise exception.InvalidInput(_("Disk %s not for data") % disk_id)
        return disk

    def _osd_create_partition_check(self, ctxt, data, key, role, disk):
        partition_id = data.get(key)
        if not partition_id:
            return
        partition = objects.DiskPartition.get_by_id(ctxt, partition_id)
        if partition.status != s_fields.DiskStatus.AVAILABLE:
            raise exception.InvalidInput(
                _("The partition %s not available") % partition_id)
        if partition.role != role:
            raise exception.InvalidInput(
                _("The partition %s not for data") % partition_id)
        if partition.node_id != disk.node_id:
            raise exception.InvalidInput(
                _("The partition is not on the same machine as the "
                  "disk") % partition_id)

    def _osd_create_check_store(self, ctxt, data):
        store = data.get('type')
        if store == s_fields.OsdType.BLUESTORE:
            return
        if (objects.sysconfig.sys_config_get(ctxt, 'ceph_version') ==
                s_fields.CephVersion.T2STOR):
            raise exception.InvalidInput(
                _("%s not support filestore") % s_fields.CephVersion.T2STOR)

    def osd_create(self, ctxt, data):
        logger.info("Osd create with %s.", data)

        # osd num check
        max_osd_num = objects.sysconfig.sys_config_get(
            ctxt, key="max_osd_num"
        )
        osd_num = objects.OsdList.get_count(ctxt)
        if osd_num >= max_osd_num:
            raise exception.InvalidInput(_("Max osd num is %s" % max_osd_num))
        # data check
        disk_id = data.get('disk_id')
        disk = self._osd_create_disk_check(ctxt, disk_id)
        # db
        self._osd_create_partition_check(
            ctxt, data, 'db_partition_id', s_fields.DiskPartitionRole.DB, disk)
        # wal
        self._osd_create_partition_check(
            ctxt, data, 'wal_partition_id',
            s_fields.DiskPartitionRole.WAL, disk)
        # cache
        self._osd_create_partition_check(
            ctxt, data, 'cache_partition_id',
            s_fields.DiskPartitionRole.CACHE, disk)
        # journal
        self._osd_create_partition_check(
            ctxt, data, 'journal_partition_id',
            s_fields.DiskPartitionRole.JOURNAL, disk)
        logger.debug("Parameter check pass.")

        # get osd id
        osd_fsid = str(uuid.uuid4())
        osd_id = self._osd_get_free_id(ctxt, osd_fsid)
        logger.info("Alloc osd id %s with osd fsid %s from ceph.",
                    osd_id, osd_fsid)

        # db create
        node_id = disk.node_id
        node = objects.Node.get_by_id(ctxt, node_id)
        osd = objects.Osd(
            ctxt, node_id=node_id,
            fsid=osd_fsid,
            osd_id=osd_id,
            type=data.get('type'),
            db_partition_id=data.get('db_partition_id'),
            wal_partition_id=data.get('wal_partition_id'),
            cache_partition_id=data.get('cache_partition_id'),
            journal_partition_id=data.get('journal_partition_id'),
            disk_id=disk_id,
            disk_type=disk.type,
            status=s_fields.OsdStatus.CREATING
        )
        osd.create()
        osd = objects.Osd.get_by_id(ctxt, osd.id, joined_load=True)
        self._set_osd_partation_role(osd)
        self._osd_config_set(ctxt, osd)

        # apply async
        self.task_submit(self._osd_create, ctxt, node, osd)
        logger.debug("Osd create task apply.")

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
            osd.db_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.db_partition.save()
        if osd.cache_partition_id:
            osd.cache_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.cache_partition.save()
        if osd.journal_partition_id:
            osd.journal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.journal_partition.save()
        if osd.wal_partition_id:
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

        osd = objects.Osd.get_by_id(ctxt, osd_id, read_deleted='yes')
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, osd, "DELETED", msg)

    def osd_delete(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        osd.status = s_fields.OsdStatus.DELETING
        osd.save()
        self.task_submit(self._osd_delete, ctxt, osd.node, osd)
        return osd

    def _update_osd_crush_id(self, ctxt, osds, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.crush_rule_id = crush_rule_id
            osd.save()
