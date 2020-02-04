import json
import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace import taskflows
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
            if size:
                osd.metrics.update({'kb': [0, size['kb']]})
                osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
                osd.metrics.update({'kb_used': [0, size['kb_used']]})
                # TODO OSD实时容量数据放到定时任务中
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

        prometheus = PrometheusTool(ctxt)
        need_osds = []
        for osd in osds:
            if not osd.need_size():
                continue
            need_osds.append(osd)
        if tab == "default":
            logger.debug("Get osd metrics: tab=default")
            prometheus.osds_get_capacity(need_osds)
            prometheus.osds_get_pg_state(need_osds)

        if tab == "io":
            logger.debug("Get osd metrics: tab=io")
            prometheus.osds_disk_perf(need_osds)

        return osds

    def osd_get_count(self, ctxt, filters=None):
        return objects.OsdList.get_count(
            ctxt, filters=filters)

    def osd_get(self, ctxt, osd_id, expected_attrs=None):
        return objects.Osd.get_by_id(ctxt, osd_id,
                                     expected_attrs=expected_attrs)

    def _osd_create(self, ctxt, node, osd, begin_action=None):
        try:
            tf = taskflows.OsdCreateTaskflow(
                ctxt, action_log_id=begin_action.id)
            tf.run(osd=osd)
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

        except Exception as e:
            logger.exception(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            res_name = osd.osd_name if osd.osd_id else osd.disk.name
            logger.info("%s create error", res_name)
            err_msg = str(e)
            msg = _("create {} error: {}").format(res_name, err_msg)
            op_status = 'CREATE_ERROR'

        res_name = osd.osd_name if osd.osd_id else osd.disk.name
        self.finish_action(begin_action, osd.id, res_name,
                           osd, osd.status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        self._osds_update_size(ctxt, [osd])
        wb_client.send_message(ctxt, osd, op_status, msg)

    def _update_disk_status(self, disk):
        update_value = {
            "status": s_fields.DiskStatus.INUSE,
        }
        expected_values = {
            "status": s_fields.DiskStatus.AVAILABLE
        }
        if not disk.conditional_update(update_value, expected_values):
            raise exception.InvalidInput(
                _("Disk(%s) status not available") % disk.name)

    def _set_osd_partation_role(self, ctxt, osd):
        self._update_disk_status(osd.disk)
        accelerate_disk = []
        if osd.db_partition_id:
            self._update_disk_status(osd.db_partition)
            accelerate_disk.append(osd.db_partition.disk_id)
        if osd.cache_partition_id:
            self._update_disk_status(osd.cache_partition)
            accelerate_disk.append(osd.cache_partition.disk_id)
        if osd.journal_partition_id:
            self._update_disk_status(osd.journal_partition)
            accelerate_disk.append(osd.journal_partition.disk_id)
        if osd.wal_partition_id:
            self._update_disk_status(osd.wal_partition)
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

    def _osd_create_disk_check(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if disk.status != s_fields.DiskStatus.AVAILABLE:
            raise exception.InvalidInput(
                _("Disk %s not available") % disk.name)
        if disk.role != s_fields.DiskRole.DATA:
            raise exception.InvalidInput(_("Disk %s not for data") % disk.name)
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

    def _osd_create_partitions_check(self, ctxt, data, disk):
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

    def _osd_create_node_check(self, ctxt, node):
        if node.status not in s_fields.NodeStatus.IDLE:
            raise exception.InvalidInput(
                _("Node status not idle"))

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
        """Osd Create

        check mon available
        check dsa available
        check node status
        """
        logger.info("Osd create with %s.", data)

        # check mon available
        # osd num check
        self._osd_create_check(ctxt, data)
        self._osd_create_check_store(ctxt, data)
        disk_id = data.get('disk_id')
        # disk check
        disk = self._osd_create_disk_check(ctxt, disk_id)
        # partitions check
        self._osd_create_partitions_check(ctxt, data, disk)
        logger.debug("Parameter check pass.")

        node_id = disk.node_id
        node = objects.Node.get_by_id(ctxt, node_id)
        # check node status
        # check agent
        self.check_agent_available(ctxt, node)

        # get osd id
        osd_fsid = str(uuid.uuid4())
        osd_id = None

        # db create
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

        # apply async
        self.task_submit(self._osd_create, ctxt, node, osd, begin_action)
        logger.debug("Osd create task apply.")

        return osd

    def _osd_delete(self, ctxt, node, osd, begin_action=None):
        res_name = osd.osd_name if osd.osd_id else osd.disk.name
        logger.info("trying to delete osd.%s", res_name)
        try:
            tf = taskflows.OsdDeleteTaskflow(
                ctxt, action_log_id=begin_action.id)
            tf.run(osd=osd)
            osd.destroy()
            msg = _("delete {} success").format(res_name)
            logger.info("osd delete {} success", res_name)
            status = 'success'
            op_status = "DELETE_SUCCESS"
            err_msg = None
        except Exception as e:
            logger.error("delete %s error: %s", res_name, e)
            status = s_fields.OsdStatus.ERROR
            osd.status = status
            osd.save()
            err_msg = str(e)
            msg = _("delete {} error: {}").format(res_name, err_msg)
            op_status = "DELETE_ERROR"
        logger.info("osd_delete, got osd: %s, osd id: %s", osd, osd.osd_id)

        self.finish_action(begin_action, osd.id, res_name,
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

        mapping = self._get_osd_df_map(ctxt)
        size = mapping.get(osd.osd_id)
        if size:
            osd.metrics.update({'kb': [0, size['kb']]})
            osd.metrics.update({'kb_avail': [0, size['kb_avail']]})
            osd.metrics.update({'kb_used': [0, size['kb_used']]})
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
            sr["slow_request_sum"] = list(filter(lambda x: x['total'] != 0,
                                                 sr["slow_request_sum"]))
            sr["slow_request_ops"] = sr["slow_request_ops"][:op_top]
        return sr

    def _osd_disk_replace_prepare(self, ctxt, osd, begin_action=None):
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
            action_sta = 'success'
            err_msg = None
        except Exception as e:
            logger.exception("osd.%s replace prepare error: %s", osd.osd_id, e)
            status = s_fields.OsdStatus.ERROR
            action_sta = 'fail'
            err_msg = str(e)
            msg = _("osd.{} replace prepare error").format(osd.osd_id)
            op_status = "OSD_CLEAN_ERROR"

        osd.status = status
        osd.save()
        if begin_action:
            self.finish_action(begin_action, osd.id, osd.osd_name,
                               after_obj=osd, status=action_sta,
                               err_msg=err_msg)
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
        self.check_mon_host(ctxt)
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        self.check_agent_available(ctxt, osd.node)
        if osd.status not in [s_fields.OsdStatus.MAINTAIN,
                              s_fields.OsdStatus.ACTIVE,
                              s_fields.OsdStatus.WARNING,
                              s_fields.OsdStatus.ERROR]:
            raise exception.InvalidInput(_("In the operation osd can "
                                           "not replace disk"))
        self._osd_replace_prepare_check(ctxt, osd)
        begin_action = self.begin_action(
            ctxt, Resource.OSD, Action.OSD_CLEAN, osd)
        osd.status = s_fields.OsdStatus.REPLACE_PREPARING
        osd.disk.status = s_fields.DiskStatus.REPLACING
        osd.disk.save()
        osd.save()
        self.task_submit(self._osd_disk_replace_prepare, ctxt, osd,
                         begin_action)
        return osd

    def _accelerate_disk_prepare(self, ctxt, disk, begin_action=None):
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
            osd_begin_action = self.begin_action(
                ctxt, Resource.OSD, Action.OSD_CLEAN, osd)
            osd.disk.status = s_fields.DiskStatus.REPLACING
            osd.disk.save()
            osd.status = s_fields.OsdStatus.REPLACE_PREPARING
            osd.save()
            osd = self._osd_disk_replace_prepare(ctxt, osd, osd_begin_action)
            if osd.status != s_fields.OsdStatus.REPLACE_PREPARED:
                osd_clean = False
        if not osd_clean:
            msg = _("accelerate disk {} osd clean error").format(disk.name)
            op_status = "DISK_CLEAN_ERROR"
            status = s_fields.DiskStatus.ERROR
            logger.error(msg)
            acction_sta = 'fail'
        else:
            msg = _("accelerate disk {} osd clean success").format(disk.name)
            op_status = "DISK_CLEAN_SUCCESS"
            status = s_fields.DiskStatus.REPLACE_PREPARED
            logger.info(msg)
            acction_sta = 'success'
        disk.status = status
        disk.save()
        self.finish_action(begin_action, disk.id, disk.name, after_obj=disk,
                           status=acction_sta)
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
                                          "wait") % osd.osd_id)
        begin_action = self.begin_action(
            ctxt, Resource.ACCELERATE_DISK, Action.ACC_DISK_CLEAN, disk)
        disk.status = s_fields.DiskStatus.REPLACE_PREPARING
        disk.save()
        self.task_submit(self._accelerate_disk_prepare, ctxt, disk,
                         begin_action)
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
            msg = _("osd.{} replace success").format(osd.osd_id)
            err_msg = None
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
            logger.info("osd.%s replace error", osd.osd_id)
            msg = _("osd.{} replace error").format(osd.osd_id)
            op_status = 'OSD_REPLACE_ERROR'
            err_msg = str(e)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, osd, op_status, msg)
        if begin_action:
            self.finish_action(
                begin_action, osd.id, osd.osd_name, osd,
                osd.status, err_msg=err_msg)

    def osd_disk_replace(self, ctxt, osd_id):
        self.check_mon_host(ctxt)
        osd = objects.Osd.get_by_id(ctxt, osd_id, joined_load=True)
        self.check_agent_available(ctxt, osd.node)
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

    def _osd_accelerate_disk_replace(self, ctxt, disk, osds, values,
                                     begin_action=None):
        logger.info("start to replace accelerate disk %s", disk.name)
        client = self.agent_manager.get_client(node_id=disk.node.id)

        partitions = client.disk_partitions_create(
            ctxt, node=disk.node, disk=disk, values=values)

        wb_client = WebSocketClientManager(context=ctxt).get_client()

        disk_partitions = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk.id})

        if not partitions or len(disk_partitions) != len(partitions):
            msg = _("create disk {} partitions failed").format(disk.name)
            op_status = "DISK_CREATE_PART_ERROR"
            logger.error(msg)
            self.finish_action(begin_action, disk.id, disk.name,
                               disk, status='fail', err_msg=msg)
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
        if disk.partition_used < disk.partition_num:
            disk.status = s_fields.DiskStatus.AVAILABLE
        else:
            disk.status = s_fields.DiskStatus.INUSE
        disk.save()
        logger.info("accelerate disk %s replace finish", disk.name)
        msg = _("accelerate disk {} replace success").format(disk.name)
        self.finish_action(begin_action, disk.id, disk.name, disk,
                           status='success')
        wb_client.send_message(ctxt, disk, "DISK_REPLACE_SUCCESS", msg)

    def osd_accelerate_disk_replace(self, ctxt, disk_id):
        self.check_mon_host(ctxt)
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used', 'node',
                                           'partitions'])
        self.check_agent_available(ctxt, disk.node)
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
        begin_action = self.begin_action(
            ctxt, Resource.ACCELERATE_DISK, Action.ACC_DISK_REBUILD, disk)
        disk.status = s_fields.DiskStatus.REPLACING
        disk.save()
        self.task_submit(
            self._osd_accelerate_disk_replace, ctxt, disk, osds, values,
            begin_action)
        return disk
