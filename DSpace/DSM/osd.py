import json
import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.prometheus import PrometheusTool
from DSpace.utils import retry

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
        not_found = []
        for osd in osds:
            if not osd.need_size():
                continue
            size = mapping.get(osd.osd_id)
            osd.metrics = {}
            if size:
                osd.metrics.update({'kb': [0, size['kb']]})
                osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
                osd.metrics.update({'kb_used': [0, size['kb_used']]})
                # TODO OSD实时容量数据放到定时任务中
                osd.size = int(size['kb']) * 1024
                osd.used = int(size['kb_used']) * 1024
                try:
                    osd.save()
                except exception.OsdNotFound:
                    logger.info("%s is deleted, not update size", osd.osd_name)
                    not_found.append(osd)
        osds = [osd for osd in osds if osd not in not_found]
        return osds

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
            osds = self._osds_update_size(ctxt, osds)
            for osd in osds:
                prometheus = PrometheusTool(ctxt)
                prometheus.osd_get_pg_state(osd)

        if tab == "io":
            logger.debug("Get osd metrics: tab=io")
            prometheus = PrometheusTool(ctxt)
            for osd in osds:
                if not osd.need_size():
                    continue
                osd.metrics = {}
                prometheus.osd_disk_perf(osd)

        return osds

    def osd_get_count(self, ctxt, filters=None):
        return objects.OsdList.get_count(
            ctxt, filters=filters)

    def osd_get(self, ctxt, osd_id, expected_attrs=None):
        return objects.Osd.get_by_id(ctxt, osd_id,
                                     expected_attrs=expected_attrs)

    @retry(exception.OsdStatusNotUp, retries=6)
    def wait_osd_up(self, ctxt, osd_name):
        # retries=6: max wait 63s
        logger.info("check osd is up: %s", osd_name)
        ceph_client = CephTask(ctxt)
        osd_tree = ceph_client.get_osd_tree()
        _osds = osd_tree.get("stray") or []
        osds = filter(lambda x: (x.get('name') == osd_name and
                                 x.get('status') == "up"),
                      _osds)
        if len(list(osds)) > 0:
            logger.info("osd is up: %s", osd_name)
            return True
        else:
            logger.info("osd not up: %s", osd_name)
            raise exception.OsdStatusNotUp()

    def _osd_create(self, ctxt, node, osd, begin_action=None):
        try:
            task = NodeTask(ctxt, node)
            osd = task.ceph_osd_install(osd)
            self.wait_osd_up(ctxt, osd.osd_name)
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
            logger.info("osd.%s create success", osd.osd_id)
            op_status = 'CREATE_SUCCESS'
            msg = _("create success: osd.{}").format(osd.osd_id)
            err_msg = None
        except exception.OsdStatusNotUp as e:
            logger.error("osd.%s create error, osd not up", osd.osd_id)
            logger.exception(e)
            osd.status = s_fields.OsdStatus.OFFLINE
            osd.save()
            msg = _("create error: osd.{} is offline").format(osd.osd_id)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)

        except exception.StorException as e:
            logger.exception(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            logger.info("osd.%s create error", osd.osd_id)
            msg = _("create error: osd.{}").format(osd.osd_id)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)

        self.finish_action(begin_action, osd.id, osd.osd_name,
                           osd, osd.status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)

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
        self.check_mon_host(ctxt)
        # osd num check
        max_osd_num = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.MAX_OSD_NUM
        )
        osd_num = objects.OsdList.get_count(ctxt)
        if osd_num >= max_osd_num:
            raise exception.InvalidInput(_("Max osd num is %s") % max_osd_num)

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
        # check agent
        self.check_agent_available(ctxt, node)
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

        self.finish_action(begin_action, osd.id, osd.osd_name,
                           osd, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        logger.debug("send websocket msg: %s", msg)

    def osd_delete(self, ctxt, osd_id):
        # check mon is ready
        self.check_mon_host(ctxt)
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        # check agent
        self.check_agent_available(ctxt, osd.node)
        if osd.status not in [s_fields.OsdStatus.ACTIVE,
                              s_fields.OsdStatus.OFFLINE,
                              s_fields.OsdStatus.REPLACE_PREPARED,
                              s_fields.OsdStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " osd can be delete"))
        begin_action = self.begin_action(
            ctxt, Resource.OSD, Action.DELETE, osd)
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

    def _osd_disk_replace_prepare(self, ctxt, osd):
        """
          prepare to replace osd disk
          1. set noout,norecover,nobackfill flag
          2. stop osd service
          3. umount osd path
          4. remove osd from cluster (don't remove osd from crush)
        """
        logger.info("prepare to replace disk for osd.%s", osd.osd_id)
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.osds_add_noout([osd.osd_id])
            task = NodeTask(ctxt, osd.node)
            osd = task.ceph_osd_clean(osd)
            status = s_fields.OsdStatus.REPLACE_PREPARED
            msg = _("osd.{} replace prepare success").format(osd.osd_id)
            op_status = "OSD_CLEAN_SUCCESS"
        except Exception as e:
            logger.exception("osd.%s replace prepare error: %s", osd.osd_id, e)
            status = s_fields.OsdStatus.ERROR
            msg = _("osd.{} replace prepare error").format(osd.osd_id)
            op_status = "OSD_CLEAN_ERROR"

        osd.status = status
        osd.save()
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        return osd

    def _osd_replace_prepare_check(self, ctxt, osd):
        accelerate_disk_ids = []
        if osd.db_partition_id:
            accelerate_disk_ids.append(osd.db_partition.disk_id)
        if osd.wal_partition_id:
            accelerate_disk_ids.append(osd.wal_partition.disk_id)
        if osd.cache_partition_id:
            accelerate_disk_ids.append(osd.cache_partition.disk_id)
        if osd.journal_partition_id:
            accelerate_disk_ids.append(osd.journal_partition.disk_id)
        accelerate_disk_ids = list(set(accelerate_disk_ids))
        for disk_id in accelerate_disk_ids:
            disk = objects.Disk.get_by_id(ctxt, disk_id)
            if not disk:
                raise exception.DiskNotFound(disk_id)
            elif disk.status in s_fields.DiskStatus.REPLACE_STATUS:
                raise exception.Invalid(_("accelerate disk %s is replacing, "
                                          "please wait" % disk.slot))

    def osd_disk_replace_prepare(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        if osd.status not in [s_fields.OsdStatus.MAINTAIN,
                              s_fields.OsdStatus.ACTIVE,
                              s_fields.OsdStatus.WARNING,
                              s_fields.OsdStatus.ERROR]:
            raise exception.InvalidInput(_("In the operation osd can "
                                           "not replace disk"))
        self._osd_replace_prepare_check(ctxt, osd)
        osd.status = s_fields.OsdStatus.REPLACE_PREPARING
        osd.disk.status = s_fields.DiskStatus.REPLACING
        osd.disk.save()
        osd.save()
        self.task_submit(self._osd_disk_replace_prepare, ctxt, osd)
        return osd

    def _accelerate_disk_prepare(self, ctxt, disk):
        logger.info("start to clean osd from accelerate disk %s", disk.name)
        osds = self._osds_get_by_accelerate_disk(ctxt, disk.id)
        osd_clean = True
        for tmp_osd in osds:
            # get osd again, insure osd status is latest
            osd = objects.Osd.get_by_id(ctxt, tmp_osd.id, joined_load=True)
            if not osd:
                continue
            if osd.status in [s_fields.OsdStatus.CREATING,
                              s_fields.OsdStatus.DELETING]:
                logger.error("can not clean osd.%s, status:"
                             " %s", osd.osd_id, osd.status)
                continue
            if osd.status == s_fields.OsdStatus.REPLACE_PREPARED:
                logger.info('osd.%s has been cleaned, continue', osd.osd_id)
                continue
            osd.disk.status = s_fields.DiskStatus.REPLACING
            osd.disk.save()
            osd.status = s_fields.OsdStatus.REPLACE_PREPARING
            osd.save()
            osd = self._osd_disk_replace_prepare(ctxt, osd)
            if osd.status != s_fields.OsdStatus.REPLACE_PREPARED:
                osd_clean = False
        if not osd_clean:
            msg = _("accelerate disk {} osd clean error").format(disk.name)
            op_status = "DISK_CLEAN_ERROR"
            status = s_fields.DiskStatus.ERROR
            logger.error(msg)
        else:
            msg = _("accelerate disk {} osd clean success").format(disk.name)
            op_status = "DISK_CLEAN_SUCCESS"
            status = s_fields.DiskStatus.REPLACE_PREPARED
            logger.info(msg)
        disk.status = status
        disk.save()
        # send websocket
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, op_status, msg)

    def osd_accelerate_disk_replace_prepare(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used', 'node',
                                           'partitions'])
        if disk.role != s_fields.DiskRole.ACCELERATE:
            raise exception.InvalidInput(_("disk role should be "
                                           "accelerate"))
        osds = self._osds_get_by_accelerate_disk(ctxt, disk.id)
        for osd in osds:
            if osd.status in s_fields.OsdStatus.REPLACE_STATUS:
                raise exception.Invalid(_("osd.%s is replacing, please "
                                          "wait" % osd.osd_id))
        disk.status = s_fields.DiskStatus.REPLACE_PREPARING
        disk.save()
        self.task_submit(self._accelerate_disk_prepare, ctxt, disk)
        return disk

    def _osd_disk_replace(self, ctxt, osd, begin_action=None):
        logger.info("replace disk for osd %s", osd.osd_id)
        try:
            task = NodeTask(ctxt, osd.node)
            osd = task.ceph_osd_replace(osd)
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
            osd.disk.status = s_fields.DiskStatus.INUSE
            osd.disk.save()
            ceph_client = CephTask(ctxt)
            ceph_client.osds_rm_noout([osd.osd_id])
            logger.info("osd.%s replace success", osd.osd_id)
            op_status = 'OSD_REPLACE_SUCCESS'
            msg = _("create success: osd.{}").format(osd.osd_id)
            err_msg = None
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            logger.info("osd.%s create error", osd.osd_id)
            msg = _("create error: osd.{}").format(osd.osd_id)
            op_status = 'OSD_REPLACE_ERROR'
            err_msg = str(e)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        if begin_action:
            self.finish_action(
                begin_action, osd.id, osd.osd_name, osd,
                osd.status, err_msg=err_msg)

    def osd_disk_replace(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        if osd.status != s_fields.OsdStatus.REPLACE_PREPARED:
            raise exception.InvalidInput(_("Osd status should be "
                                           "REPLACE_PREPARED"))
        # check db/cache/journal status
        begin_action = self.begin_action(
            ctxt, Resource.OSD, Action.OSD_REPLACE, osd)
        osd.status = s_fields.OsdStatus.REPLACING
        osd.save()
        # apply async
        self.task_submit(self._osd_disk_replace, ctxt, osd, begin_action)
        logger.debug("Osd create task apply.")

        return osd

    def _osd_accelerate_disk_replace(self, ctxt, disk, osds, values):
        logger.info("start to replace accelerate disk %s", disk.name)
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client(node_id=disk.node.id)

        partitions = client.disk_partitions_create(
            ctxt, node=disk.node, disk=disk, values=values)

        wb_client = WebSocketClientManager(context=ctxt).get_client()

        disk_partitions = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk.id})

        if not partitions or len(disk_partitions) != len(partitions):
            msg = _("create disk {} partitions failed").format(disk.name)
            op_status = "DISK_CREATE_PART_ERROR"
            logger.error(msg)
            wb_client.send_message(ctxt, disk, op_status, msg)
            return

        for disk_part in disk_partitions:
            for part in partitions:
                if disk_part.role == part.get('role'):
                    disk_part.uuid = part.get('uuid')
                    disk_part.size = part.get('size')
                    disk_part.name = part.get('name')
                    disk_part.save()
                    partitions.remove(part)
                    break
        for tmp_osd in osds:
            # get osd again, insure osd status is latest
            osd = objects.Osd.get_by_id(ctxt, tmp_osd.id, joined_load=True)
            osd.status = s_fields.OsdStatus.REPLACING
            osd.save()
            self._osd_disk_replace(ctxt, osd)
        disk.status = s_fields.DiskStatus.AVAILABLE
        disk.save()
        logger.info("accelerate disk %s replace finish", disk.name)
        msg = _("accelerate disk {} replace success").format(disk.name)
        wb_client.send_message(ctxt, disk, "DISK_REPLACE_SUCCESS", msg)

    def osd_accelerate_disk_replace(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used', 'node',
                                           'partitions'])
        if not disk.partitions:
            raise exception.InvalidInput(_("accelerate disk has no "
                                           "partitions"))
        accelerate_role = []
        for partition in disk.partitions:
            if partition.role not in accelerate_role:
                accelerate_role.append(partition.role)
        values = {}
        if len(accelerate_role) > 1:
            values['partition_role'] = s_fields.DiskPartitionRole.MIX
            values['partition_num'] = disk.partition_num / 2
        else:
            values['partition_role'] = accelerate_role[0]
            values['partition_num'] = disk.partition_num
        osds = self._osds_get_by_accelerate_disk(ctxt, disk.id)
        for osd in osds:
            if osd.status != s_fields.OsdStatus.REPLACE_PREPARED:
                raise exception.InvalidInput(_("Osd status should be "
                                               "REPLACE_PREPARED"))
        disk.status = s_fields.DiskStatus.REPLACING
        disk.save()
        self.task_submit(
            self._osd_accelerate_disk_replace, ctxt, disk, osds, values)
        return disk
