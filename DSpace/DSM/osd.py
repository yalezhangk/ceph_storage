import json
import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class OsdHandler(AdminBaseHandler):
    def _osds_get_by_accelerate_disk(self, ctxt, disk_id, expected_attrs=None):
        disk_partitions = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk_id})
        res = []
        for partition in disk_partitions:
            if partition.role == s_fields.DiskPartitionRole.DB:
                filters = {'db_partition_id': partition.id}
            if partition.role == s_fields.DiskPartitionRole.WAL:
                filters = {'wal_partition_id': partition.id}
            if partition.role == s_fields.DiskPartitionRole.CACHE:
                filters = {'cache_partition_id': partition.id}
            if partition.role == s_fields.DiskPartitionRole.JOURNAL:
                filters = {'journal_partition_id': partition.id}
            osds = objects.OsdList.get_all(
                ctxt, filters=filters, expected_attrs=expected_attrs)
            for osd in osds:
                if osd in res:
                    continue
                res.append(osd)
        return res

    def _get_osd_df_map(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            capacity = ceph_client.get_osd_df()
        else:
            capacity = None
        mapping = {}
        if capacity:
            osd_capacity = capacity.get('nodes') + capacity.get('stray')
            logger.info("get osd_capacity: %s", json.dumps(osd_capacity))
            for c in osd_capacity:
                mapping[str(c['id'])] = c
        return mapping

    def _osds_update_size(self, ctxt, osds):
        mapping = self._get_osd_df_map(ctxt)
        for osd in osds:
            size = mapping.get(osd.osd_id)
            osd.metrics = {}
            if size:
                osd.metrics.update({'kb': [0, size['kb']]})
                osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
                osd.metrics.update({'kb_used': [0, size['kb_used']]})
                # TODO OSD实时容量数据放到定时任务中
                osd.size = int(size['kb']) * 1024
                osd.used = int(size['kb_used']) * 1024
                osd.save()

    def osd_get_all(self, ctxt, tab=None, marker=None, limit=None,
                    sort_keys=None, sort_dirs=None, filters=None, offset=None,
                    expected_attrs=None):
        disk_id = filters.get('disk_id')
        if disk_id:
            osds = self._osds_get_by_accelerate_disk(
                ctxt, disk_id, expected_attrs=expected_attrs)
        else:
            osds = objects.OsdList.get_all(
                ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
                sort_dirs=sort_dirs, filters=filters, offset=offset,
                expected_attrs=expected_attrs)

        if not osds:
            return osds

        if tab == "default":
            logger.debug("Get osd metrics: tab=default")
            self._osds_update_size(ctxt, osds)
            for osd in osds:
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

    def _osd_create(self, ctxt, node, osd, begin_action=None):
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_install(osd)
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
            logger.info("osd.%s create success", osd.osd_id)
            op_status = 'CREATE_SUCCESS'
            msg = _("create success: osd.{}").format(osd.osd_id)
            err_msg = None
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            logger.info("osd.%s create error", osd.osd_id)
            msg = _("create error: osd.{}").format(osd.osd_id)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        self.finish_action(begin_action, osd.id, 'osd.{}'.format(osd.osd_id),
                           objects.json_encode(node), osd.status,
                           err_msg=err_msg)

    def _set_osd_partation_role(self, ctxt, osd):
        osd.disk.status = s_fields.DiskStatus.INUSE
        osd.disk.save()
        accelerate_disk = []
        if osd.db_partition_id:
            osd.db_partition.status = s_fields.DiskStatus.INUSE
            osd.db_partition.save()
            accelerate_disk.append(osd.db_partition.disk_id)
        if osd.cache_partition_id:
            osd.cache_partition.status = s_fields.DiskStatus.INUSE
            osd.cache_partition.save()
            accelerate_disk.append(osd.cache_partition.disk_id)
        if osd.journal_partition_id:
            osd.journal_partition.status = s_fields.DiskStatus.INUSE
            osd.journal_partition.save()
            accelerate_disk.append(osd.journal_partition.disk_id)
        if osd.wal_partition_id:
            osd.wal_partition.status = s_fields.DiskStatus.INUSE
            osd.wal_partition.save()
            accelerate_disk.append(osd.wal_partition.disk_id)
        accelerate_disk = set(accelerate_disk)
        if accelerate_disk:
            ac_disk = objects.DiskList.get_all(
                ctxt, filters={'id': accelerate_disk},
                expected_attrs=['partition_used'])
            for disk in ac_disk:
                if disk.partition_used < disk.partition_num:
                    disk.status = s_fields.DiskStatus.AVAILABLE
                else:
                    disk.status = s_fields.DiskStatus.INUSE
                disk.save()

    def _osd_get_free_id(self, ctxt, osd_fsid):
        task = CephTask(ctxt)
        return task.osd_new(osd_fsid)

    def _osd_config_set(self, ctxt, osd):
        if osd.cache_partition_id:
            ceph_cfg = objects.CephConfig(
                ctxt, group="osd.%s" % osd.osd_id,
                key='backend_type',
                value='t2ce',
                value_type=s_fields.ConfigType.STRING
            )
            ceph_cfg.create()
        ceph_cfg = objects.CephConfig(
            ctxt, group="osd.%s" % osd.osd_id,
            key='osd_objectstore',
            value=osd.type,
            value_type=s_fields.ConfigType.STRING
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
        ceph_version = objects.sysconfig.sys_config_get(
            ctxt, 'ceph_version_name')
        if (ceph_version == s_fields.CephVersion.T2STOR):
            if data.get('type') == s_fields.OsdType.FILESTORE:
                raise exception.InvalidInput(
                    _("%s not support filestore") % ceph_version)
        else:
            if data.get('cache_partition_id'):
                raise exception.InvalidInput(
                    _("%s not support cache") % ceph_version)

    def _osd_create_check(self, ctxt, data):
        # check mon is ready
        active_mon_num = objects.NodeList.get_count(
            ctxt,
            filters={
                "role_monitor": True,
                "status": s_fields.NodeStatus.ACTIVE
            }
        )
        if not active_mon_num:
            raise exception.InvalidInput(_("No active monitor"))
        # osd num check
        max_osd_num = objects.sysconfig.sys_config_get(
            ctxt, key="max_osd_num"
        )
        osd_num = objects.OsdList.get_count(ctxt)
        if osd_num >= max_osd_num:
            raise exception.InvalidInput(_("Max osd num is %s" % max_osd_num))

    def osd_create(self, ctxt, data):
        logger.info("Osd create with %s.", data)

        self._osd_create_check(ctxt, data)
        self._osd_create_check_store(ctxt, data)
        # data check
        disk_id = data.get('disk_id')
        disk = self._osd_create_disk_check(ctxt, disk_id)
        # db
        self._osd_create_partition_check(
            ctxt, data, 'db_partition_id',
            s_fields.DiskPartitionRole.DB, disk)
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
        begin_action = self.begin_action(ctxt, Resource.OSD, Action.CREATE)
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
        self._set_osd_partation_role(ctxt, osd)
        self._osd_config_set(ctxt, osd)

        # apply async
        self.task_submit(self._osd_create, ctxt, node, osd, begin_action)
        logger.debug("Osd create task apply.")

        return osd

    def _osd_config_remove(self, ctxt, osd):
        logger.debug("osd clear config")
        osd_cfgs = objects.CephConfigList.get_all(
            ctxt, filters={'group': "osd.%s" % osd.osd_id}
        )
        for cfg in osd_cfgs:
            cfg.destroy()

    def _osd_clear_partition_role(self, ctxt, osd):
        logger.debug("osd clear partition role")
        osd.disk.status = s_fields.DiskStatus.AVAILABLE
        osd.disk.save()

        accelerate_disk = []
        if osd.db_partition_id:
            osd.db_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.db_partition.save()
            accelerate_disk.append(osd.db_partition.disk_id)
        if osd.cache_partition_id:
            osd.cache_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.cache_partition.save()
            accelerate_disk.append(osd.cache_partition.disk_id)
        if osd.journal_partition_id:
            osd.journal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.journal_partition.save()
            accelerate_disk.append(osd.journal_partition.disk_id)
        if osd.wal_partition_id:
            osd.wal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.wal_partition.save()
            accelerate_disk.append(osd.wal_partition.disk_id)
        accelerate_disk = set(accelerate_disk)
        if accelerate_disk:
            ac_disk = objects.DiskList.get_all(
                ctxt, filters={'id': accelerate_disk})
            for disk in ac_disk:
                disk.status = s_fields.DiskStatus.AVAILABLE
                disk.save()

    def _osd_delete(self, ctxt, node, osd, begin_action=None):
        logger.info("trying to delete osd.%s", osd.osd_id)
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_uninstall(osd)
            self._osd_config_remove(ctxt, osd)
            self._osd_clear_partition_role(ctxt, osd)
            osd.destroy()
            msg = _("delete osd.{} success").format(osd.osd_id)
            logger.info("delete osd.%s success", osd.osd_id)
            status = 'success'
            op_status = "DELETE_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            logger.error("delete osd.%s error: %s", osd.osd_id, e)
            status = s_fields.OsdStatus.ERROR
            osd.status = status
            osd.save()
            err_msg = str(e)
            msg = _("delete osd.{} error").format(osd.osd_id)
            op_status = "DELETE_ERROR"
        logger.info("osd_delete, got osd: %s, osd id: %s", osd, osd.osd_id)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, osd.id, 'osd.{}'.format(osd.osd_id),
                           objects.json_encode(osd), status,
                           err_msg=err_msg)

    def osd_delete(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        if osd.status not in [s_fields.OsdStatus.ACTIVE,
                              s_fields.OsdStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " osd can be delete"))
        begin_action = self.begin_action(ctxt, Resource.OSD, Action.DELETE)
        osd.status = s_fields.OsdStatus.DELETING
        osd.save()
        self.task_submit(self._osd_delete, ctxt, osd.node, osd, begin_action)
        return osd

    def _update_osd_crush_id(self, ctxt, osds, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.crush_rule_id = crush_rule_id
            osd.save()

    def get_disk_info(self, ctxt, data):
        disk_id = data.get('disk_id')
        disk = objects.Disk.get_by_id(ctxt, disk_id, expected_attrs=['node'])
        disk_name = disk.name
        hostname = disk.node.hostname
        return {'hostname': hostname, 'disk_name': disk_name}

    def osd_capacity_get(self, ctxt, osd_id):
        logger.info("Osd capacity get: osd_id: %s.", osd_id)
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        osd.metrics = {}

        mapping = self._get_osd_df_map(ctxt)
        size = mapping.get(osd.osd_id)
        osd.metrics = {}
        if size:
            osd.metrics.update({'kb': [0, size['kb']]})
            osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
            osd.metrics.update({'kb_used': [0, size['kb_used']]})
            osd.size = int(size['kb']) * 1024
            osd.used = int(size['kb_used']) * 1024
            osd.save()

        prometheus.osd_get_bluefs_capacity(osd)
        return osd.metrics

    def osd_slow_requests_get(self, ctxt, osd_top=10, op_top=10):
        if not isinstance(osd_top, int) or not isinstance(osd_top, int):
            raise exception.InvalidInput(_("Invalid osd_top or op_top."))
        logger.info("Get osd slow request...")
        sr = self.slow_requests.get(ctxt.cluster_id)
        if sr:
            sr["slow_request_sum"] = sr["slow_request_sum"][:osd_top]
            sr["slow_request_ops"] = sr["slow_request_ops"][:op_top]
        return sr
