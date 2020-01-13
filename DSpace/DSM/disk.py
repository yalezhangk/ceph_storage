import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.node import NodeTask
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class DiskHandler(AdminBaseHandler):
    def disk_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used', 'node',
                                           'partitions'])
        return disk

    def disk_get_all(self, ctxt, tab=None, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        disks = objects.DiskList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=['node', 'partition_used', 'partitions'])
        if tab == "io":
            prometheus = PrometheusTool(ctxt)
            for disk in disks:
                prometheus.disk_get_perf(disk)
        if filters.get("role") == "accelerate":
            for disk in disks:
                if not disk.partitions:
                    disk.accelerate_type = None
                else:
                    accelerate_role = []
                    for partition in disk.partitions:
                        if partition.role not in accelerate_role:
                            accelerate_role.append(partition.role)
                    if len(accelerate_role) > 1:
                        disk.accelerate_type = s_fields.DiskPartitionRole.MIX
                    else:
                        disk.accelerate_type = accelerate_role[0]
        return disks

    def disk_get_count(self, ctxt, filters=None):
        return objects.DiskList.get_count(ctxt, filters=filters)

    def disk_update(self, ctxt, disk_id, disk_type):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        begin_action = self.begin_action(ctxt, Resource.DISK,
                                         Action.CHANGE_DISK_TYPE, disk)
        disk.type = disk_type
        disk.save()
        self.finish_action(begin_action, disk_id, disk.name,
                           after_obj=disk)
        return disk

    def disk_light(self, ctxt, disk_id, led):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if disk.support_led:
            if disk.led == led:
                raise exception.InvalidInput(
                    reason=_("disk: repeated actions, led is {}".format(led)))
            node = objects.Node.get_by_id(ctxt, disk.node_id)
            begin_action = self.begin_action(ctxt, Resource.DISK,
                                             Action.DISK_LIGHT, disk)
            client = self.agent_manager.get_client(node_id=disk.node_id)
            _success = client.disk_light(ctxt, led=led, node=node,
                                         name=disk.name)
            if _success:
                disk.led = led
                disk.save()
                status = 'success'
            else:
                status = 'fail'
            self.finish_action(begin_action, disk_id, disk.name,
                               disk, status)
        else:
            raise exception.LedNotSupport(disk_id=disk_id)
        return disk

    def _disk_partitions_create(self, ctxt, node, disk, values,
                                begin_action=None):
        client = self.agent_manager.get_client(node_id=disk.node_id)
        partitions = client.disk_partitions_create(ctxt, node=node, disk=disk,
                                                   values=values)
        if partitions:
            partitions_old = objects.DiskPartitionList.get_all(
                ctxt, filters={'disk_id': disk.id})
            if partitions_old:
                for part in partitions_old:
                    part.destroy()

            if values['partition_role'] == s_fields.DiskPartitionRole.MIX:
                disk.partition_num = values['partition_num'] * 2
            else:
                disk.partition_num = values['partition_num']
            disk.role = values['role']
            for part in partitions:
                partition = objects.DiskPartition(
                    ctxt, name=part.get('name'), size=part.get('size'),
                    status="available", type=disk.type, uuid=part.get('uuid'),
                    role=part.get('role'), node_id=disk.node_id,
                    disk_id=disk.id, cluster_id=disk.cluster_id,
                )
                partition.create()
            msg = _("create disk partitions success")
            op_status = "CREATE_PART_SUCCESS"
            action_status = 'success'
            status = s_fields.DiskStatus.AVAILABLE
        else:
            msg = _("create disk partitions failed")
            action_status = 'fail'
            status = s_fields.DiskStatus.ERROR
            op_status = "CREATE_PART_ERROR"
        disk.conditional_update({
            "status": status
        }, expected_values={
            "status": s_fields.DiskStatus.PROCESSING
        }, save_all=True)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, op_status, msg)
        self.finish_action(begin_action, disk.id, disk.name,
                           disk, action_status)

    def disk_partitions_create(self, ctxt, disk_id, values):
        ceph_version = objects.sysconfig.sys_config_get(
            ctxt, 'ceph_version_name')
        if (ceph_version == s_fields.CephVersion.T2STOR):
            t2stor_support_type = [
                s_fields.DiskPartitionRole.DB,
                s_fields.DiskPartitionRole.CACHE,
                s_fields.DiskPartitionRole.MIX
            ]
            if values['partition_role'] not in t2stor_support_type:
                raise exception.InvalidInput(_("Partition type not support"))
        else:
            luminous_support_type = [
                s_fields.DiskPartitionRole.DB,
                s_fields.DiskPartitionRole.WAL,
                s_fields.DiskPartitionRole.JOURNAL
            ]
            if values['partition_role'] not in luminous_support_type:
                raise exception.InvalidInput(_("Partition type not support"))
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if not disk.can_operation():
            raise exception.InvalidInput(_("Disk status not available"))
        disk.conditional_update({
            "status": s_fields.DiskStatus.PROCESSING
        }, expected_values={
            "status": disk.can_operation_status
        })
        begin_action = self.begin_action(
            ctxt, Resource.DISK, Action.CREATE, disk)
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        self.task_submit(self._disk_partitions_create, ctxt, node, disk,
                         values, begin_action)
        return disk

    def _disk_partitions_remove(self, ctxt, node, disk, values,
                                begin_action=None):
        client = self.agent_manager.get_client(node_id=disk.node_id)
        _success = client.disk_partitions_remove(ctxt, node=node,
                                                 name=disk.name, )
        if _success:
            logger.info("Disk partitions remove: Successful")
            partitions_old = objects.DiskPartitionList.get_all(
                ctxt, filters={'disk_id': disk.id})
            if partitions_old:
                for part in partitions_old:
                    part.destroy()
            disk.partition_num = 0
            disk.role = values['role']
            status = 'available'
            msg = _("remove disk partitions success")
            op_status = "REMOVE_PART_SUCCESS"
        else:
            logger.error("Disk partitions remove: Failed")
            msg = _("remove disk partitions failed")
            status = 'fail'
            op_status = "REMOVE_PART_ERROR"
        disk.conditional_update({
            "status": s_fields.DiskStatus.AVAILABLE
        }, expected_values={
            "status": s_fields.DiskStatus.PROCESSING
        }, save_all=True)
        self.finish_action(begin_action, disk.id, disk.name,
                           disk, status)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, op_status, msg)

    def disk_partitions_remove(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used'])
        # 分区不为0，不可被删
        if disk.partition_used:
            raise exception.InvalidInput(
                reason=_('current disk:{} partition has used, '
                         'can not del').format(disk.name))
        if not disk.can_operation():
            raise exception.InvalidInput(_("Disk status not available"))
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        begin_action = self.begin_action(
            ctxt, Resource.DISK, Action.DELETE, disk)
        disk.conditional_update({
            "status": s_fields.DiskStatus.PROCESSING
        }, expected_values={
            "status": disk.can_operation_status
        })
        self.task_submit(self._disk_partitions_remove, ctxt, node, disk,
                         values, begin_action)
        return disk

    def disk_smart_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if not disk.name:
            return []
        client = self.agent_manager.get_client(node_id=disk.node_id)
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        smart = client.disk_smart_get(ctxt, node=node, name=disk.name)
        return smart

    def disk_partition_get_all(self, ctxt, marker=None, limit=None,
                               sort_keys=None, sort_dirs=None, filters=None,
                               offset=None, expected_attrs=None):
        disks = objects.DiskPartitionList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
        return disks

    def disk_partition_get_count(self, ctxt, filters=None):
        return objects.DiskPartitionList.get_count(
            ctxt, filters=filters)

    def disk_partitions_reporter(self, ctxt, partitions, disk):
        all_partition_objs = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk.id})
        all_partitions = {
            partition.name: partition for partition in all_partition_objs
        }
        for name, data in six.iteritems(partitions):
            if name in all_partitions:
                partition = all_partitions.pop(name)
                partition.size = data.get('size')
                partition.save()
            else:
                partition = objects.DiskPartition(
                    ctxt,
                    name=name,
                    size=data.get('size'),
                    status=s_fields.DiskStatus.AVAILABLE,
                    type=data.get('type', s_fields.DiskType.HDD),
                    node_id=disk.node_id,
                    disk_id=disk.id,
                )
                partition.create()
        for name, partition in six.iteritems(all_partitions):
            logger.warning("Remove partition %s", name)
            partition.destroy()

    def _osd_get_by_accelerate_partition(self,
                                         ctxt,
                                         partition,
                                         expected_attrs=None):
        if partition.role == s_fields.DiskPartitionRole.DB:
            filters = {'db_partition_id': partition.id}
        elif partition.role == s_fields.DiskPartitionRole.WAL:
            filters = {'wal_partition_id': partition.id}
        elif partition.role == s_fields.DiskPartitionRole.CACHE:
            filters = {'cache_partition_id': partition.id}
        elif partition.role == s_fields.DiskPartitionRole.JOURNAL:
            filters = {'journal_partition_id': partition.id}
        else:
            return None
        osds = objects.OsdList.get_all(
            ctxt, filters=filters, expected_attrs=expected_attrs)
        if len(osds):
            return osds[0]
        else:
            return None

    def disk_offline(self, ctxt, slot, node_id):
        logger.info("recieve disk offline on %s: %s", node_id, slot)
        disk = objects.Disk.get_by_slot(
            ctxt, slot, node_id,
            expected_attrs=['node', 'partition_used', 'partitions'])
        if not disk:
            logger.info('No disk found, slot: %s', slot)
            return
        # send websocket
        msg = _('disk %s offline') % disk.name
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, 'DISK_OFFLINE', msg)
        # create alert log
        alert_rules = objects.AlertRuleList.get_all(
            ctxt,
            filters={'type': 'disk_offline', 'cluster_id': ctxt.cluster_id})
        if not alert_rules:
            logger.info('alert_rule: disk_offline not found')
        else:
            alert_rule = alert_rules[0]
            if not alert_rule.enabled:
                logger.info('alert_rule:%s not enable', alert_rule.type)
            else:
                alert_log_data = {
                    'resource_type': alert_rule.resource_type,
                    'resource_name': disk.name,
                    'resource_id': disk.id,
                    'level': alert_rule.level,
                    'alert_value': alert_rule.trigger_value,
                    'alert_rule_id': alert_rule.id,
                    'cluster_id': ctxt.cluster_id
                }
                alert_log = objects.AlertLog(ctxt, **alert_log_data)
                alert_log.create()
                logger.info('alert %s happen: %s', alert_log.resource_type,
                            alert_log.alert_value)
        if disk.role == s_fields.DiskRole.ACCELERATE:
            if disk.status in s_fields.DiskStatus.REPLACE_STATUS:
                logger.info("%s in replacing task, do nothing", disk.name)
            else:
                for partition in disk.partitions:
                    osd = self._osd_get_by_accelerate_partition(
                        ctxt, partition, expected_attrs=['node'])
                    if (osd and
                            (osd.status not in
                             s_fields.OsdStatus.REPLACE_STATUS)):
                        logger.info('accelerate disk has osd.%s, set offline',
                                    osd.osd_id)
                        task = NodeTask(ctxt, osd.node)
                        task.ceph_osd_offline(osd, umount=False)
                        osd.status = s_fields.OsdStatus.OFFLINE
                        osd.save()
                    partition.name = None
                    partition.save()
                disk.status = s_fields.DiskStatus.ERROR
        elif disk.role == s_fields.DiskRole.DATA:
            if disk.status == s_fields.DiskStatus.REPLACING:
                logger.info("disk pull out")
            else:
                osds = objects.OsdList.get_all(
                    ctxt,
                    filters={'disk_id': disk.id},
                    expected_attrs=['node'])
                if len(osds):
                    # stop osd
                    osd = osds[0]
                    logger.info('disk has osd.%s, set offline', osd.osd_id)
                    task = NodeTask(ctxt, osd.node)
                    task.ceph_osd_offline(osd, umount=True)
                    osd.status = s_fields.OsdStatus.OFFLINE
                    osd.save()
                logger.info('disk %s pull out, mark it error', disk.name)
                disk.status = s_fields.DiskStatus.ERROR
        else:
            logger.error("Disk role type error")
        disk.name = None
        disk.save()

    def _disk_add_new(self, ctxt, disk_info, node_id):
        logger.info("Create node_id %s disk %s: %s",
                    node_id, disk_info.get('name'), disk_info)
        if len(disk_info.get('partitions')):
            logger.info("disk %s has partitions, set status to unavailable",
                        disk_info.get('name'))
            status = s_fields.DiskStatus.UNAVAILABLE
        else:
            status = s_fields.DiskStatus.AVAILABLE

        disk = objects.Disk(
            ctxt,
            name=disk_info.get('name'),
            status=status,
            type=disk_info.get('type'),
            size=disk_info.get('size'),
            slot=disk_info.get('slot'),
            node_id=node_id,
            partition_num=len(disk_info.get('partitions')),
            role=s_fields.DiskRole.DATA
        )
        disk.create()

    def _disk_update_accelerate(self, ctxt, disk, disk_info, node_id):
        logger.info('update accelerate disk: %s', disk.name)
        if disk.status == s_fields.DiskStatus.REPLACING:
            logger.info("%s in replacing task, do nothing", disk.name)
        else:
            # check disk part table
            partitions = disk_info.get('partitions')
            if len(partitions) != disk.partition_num:
                msg = _('disk %s plug partition different') % disk.name
                logger.error(msg)
                wb_client = WebSocketClientManager(context=ctxt).get_client()
                wb_client.send_message(ctxt, disk, 'DISK_ONLINE_ERROR', msg)
                disk.status = s_fields.DiskStatus.ERROR
                return
            disk_partitions = []
            for partition in partitions:
                disk_partition = objects.DiskPartition.get_by_uuid(
                    ctxt, partition.get('uuid'), disk.node_id,
                    expected_attrs=['node', 'disk'])
                if not disk_partition:
                    msg = _('disk %s plug partition different') % disk.name
                    logger.error(msg)
                    wb_client = WebSocketClientManager(
                        context=ctxt).get_client()
                    wb_client.send_message(
                        ctxt, disk, 'DISK_ONLINE_ERROR', msg)
                    disk.status = s_fields.DiskStatus.ERROR
                    return
                disk_partition.name = partition.get('name')
                disk_partition.save()
                disk_partitions.append(disk_partition)

            # update disk partition info
            disk_status = s_fields.DiskStatus.AVAILABLE
            for disk_partition in disk_partitions:
                osd = self._osd_get_by_accelerate_partition(
                    ctxt, disk_partition, expected_attrs=['node'])
                if not osd:
                    disk_partition.status = s_fields.DiskStatus.AVAILABLE
                elif osd.status in s_fields.OsdStatus.REPLACE_STATUS:
                    logger.info('osd.%s is replacing, do not restart',
                                osd.osd_id)
                    disk_partition.status = s_fields.DiskStatus.INUSE
                    disk_status = s_fields.DiskStatus.INUSE
                else:
                    logger.info('accelerate disk has osd.%s, restarting',
                                osd.osd_id)
                    task = NodeTask(ctxt, osd.node)
                    task.ceph_osd_restart(osd)
                    osd.status = s_fields.OsdStatus.ACTIVE
                    osd.save()
                    disk_partition.status = s_fields.DiskStatus.INUSE
                    disk_status = s_fields.DiskStatus.INUSE
                disk_partition.save()
            disk.status = disk_status
        disk.type = s_fields.DiskType.SSD
        disk.save()

    def _disk_update_data(self, ctxt, disk, disk_info, node_id):
        logger.info('update data disk: %s', disk.name)
        osds = objects.OsdList.get_all(
            ctxt, filters={'disk_id': disk.id}, expected_attrs=['node'])
        if disk.status in s_fields.DiskStatus.REPLACE_STATUS:
            logger.info("%s in replacing task, do nothing", disk.name)
        elif len(osds):
            osd = osds[0]
            logger.info('disk has osd.%s, set status to active', osd.osd_id)
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
            disk.status = s_fields.DiskStatus.INUSE
        else:
            if len(disk_info.get('partitions')):
                disk.status = s_fields.DiskStatus.UNAVAILABLE
                disk.partition_num = len(disk_info.get('partitions'))
            else:
                disk.status = s_fields.DiskStatus.AVAILABLE
        disk.save()

    def disk_online(self, ctxt, disk_info, node_id):
        logger.info("recieve disk online on %s: %s", node_id, disk_info)
        disk = objects.Disk.get_by_slot(
            ctxt, disk_info.get('slot'), node_id,
            expected_attrs=['node', 'partition_used', 'partitions'])

        if not disk:
            self._disk_add_new(ctxt, disk_info, node_id)
        else:
            disk.name = disk_info.get('name')
            disk.type = disk_info.get('type')
            disk.size = disk_info.get('size')
            disk.save()
            if disk.role == s_fields.DiskRole.ACCELERATE:
                self._disk_update_accelerate(ctxt, disk, disk_info, node_id)
            elif disk.role == s_fields.DiskRole.DATA:
                self._disk_update_data(ctxt, disk, disk_info, node_id)
            else:
                logger.error("Disk role type error")
        # send websocket
        msg = _('disk %s online') % disk.name
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, 'DISK_ONLINE', msg)
        # create alert log
        alert_rules = objects.AlertRuleList.get_all(
            ctxt,
            filters={'type': 'disk_online', 'cluster_id': ctxt.cluster_id})
        if not alert_rules:
            logger.info('alert_rule:disk_online not found')
        else:
            alert_rule = alert_rules[0]
            if not alert_rule.enabled:
                logger.info('alert_rule:%s not enable', alert_rule.type)
            else:
                alert_log_data = {
                    'resource_type': alert_rule.resource_type,
                    'resource_name': disk.name,
                    'resource_id': disk.id,
                    'level': alert_rule.level,
                    'alert_value': alert_rule.trigger_value,
                    'alert_rule_id': alert_rule.id,
                    'cluster_id': ctxt.cluster_id
                }
                alert_log = objects.AlertLog(ctxt, **alert_log_data)
                alert_log.create()
                logger.info('alert %s happen: %s', alert_log.resource_type,
                            alert_log.alert_value)

    def disk_reporter(self, ctxt, disks, node_id):
        all_disk_objs = objects.DiskList.get_all(
            ctxt, filters={'node_id': node_id},
            expected_attrs=['partition_used']
        )
        all_disks = {
            disk.slot: disk for disk in all_disk_objs
        }
        for slot, data in six.iteritems(disks):
            logger.info("Check node_id %s disk %s: %s",
                        node_id, data.get('name'), data)
            partitions = data.get("partitions")
            if slot in all_disks:
                disk = all_disks.pop(slot)
                if disk.status == s_fields.DiskStatus.REPLACING:
                    continue
                osds = objects.OsdList.get_all(
                    ctxt, filters={'disk_id': disk.id})
                osd_disk = False
                if osds:
                    osd_disk = True

                disk.size = int(data.get('size'))
                disk.partition_num = len(partitions)
                if disk.partition_num:
                    for k, v in six.iteritems(partitions):
                        part = objects.DiskPartitionList.get_all(
                            ctxt, filters={'name': k, 'node_id': node_id})
                        if not part:
                            continue
                        if part[0].role in [s_fields.DiskPartitionRole.CACHE,
                                            s_fields.DiskPartitionRole.DB,
                                            s_fields.DiskPartitionRole.JOURNAL,
                                            s_fields.DiskPartitionRole.WAL]:
                            if disk.partition_used < disk.partition_num:
                                disk.status = s_fields.DiskStatus.AVAILABLE
                            else:
                                disk.status = s_fields.DiskStatus.INUSE
                        elif osd_disk:
                            disk.status = s_fields.DiskStatus.INUSE
                            parts = objects.DiskPartitionList.get_all(
                                ctxt, filters={'disk_id': osds[0].disk_id})
                            for part in parts:
                                part.status = s_fields.DiskStatus.INUSE
                                part.role = s_fields.DiskPartitionRole.DATA
                                part.save()
                        elif data.get('is_sys_dev'):
                            disk.status = s_fields.DiskStatus.INUSE
                        else:
                            disk.status = s_fields.DiskStatus.UNAVAILABLE
                elif data.get('mounted'):
                    disk.status = s_fields.DiskStatus.UNAVAILABLE
                else:
                    disk.status = s_fields.DiskStatus.AVAILABLE
                disk.name = data.get('name')
                disk.size = data.get('size')
                disk.save()
                logger.info("Update node_id %s disk %s: %s",
                            node_id, slot, data)
            else:
                if data.get('is_sys_dev'):
                    status = s_fields.DiskStatus.INUSE
                    role = s_fields.DiskRole.SYSTEM
                elif len(partitions) or data.get('mounted'):
                    status = s_fields.DiskStatus.UNAVAILABLE
                    role = s_fields.DiskRole.DATA
                else:
                    status = s_fields.DiskStatus.AVAILABLE
                    role = s_fields.DiskRole.DATA

                disk = objects.Disk(
                    ctxt,
                    name=data.get('name'),
                    status=status,
                    type=data.get('type', s_fields.DiskType.HDD),
                    size=data.get('size'),
                    rotate_speed=data.get('rotate_speed'),
                    slot=data.get('slot'),
                    node_id=node_id,
                    partition_num=len(partitions),
                    role=role
                )
                disk.create()
                logger.info("Create node_id %s disk %s: %s",
                            node_id, data.get('name'), data)
            # TODO slot has no disk
            self.disk_partitions_reporter(ctxt, partitions, disk)
        for slot, disk in six.iteritems(all_disks):
            logger.warning("slot %s disk %s not found, mark it error",
                           slot, disk.name)
            disk.name = None
            status = s_fields.DiskStatus.ERROR
            disk.save()

    def disk_get_all_available(self, ctxt, filters=None, expected_attrs=None):
        filters['status'] = s_fields.DiskStatus.AVAILABLE
        filters['role'] = s_fields.DiskRole.DATA
        disks = objects.DiskList.get_all_available(
            ctxt, filters=filters, expected_attrs=expected_attrs)
        return disks

    def disk_partition_get_all_available(self, ctxt, filters=None,
                                         expected_attrs=None):
        filters['status'] = s_fields.DiskStatus.AVAILABLE
        partitions = objects.DiskPartitionList.get_all_available(
            ctxt, filters=filters, expected_attrs=expected_attrs)
        return partitions

    def disk_io_top(self, ctxt, k=10):
        logger.info("disk io top(%s)", k)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.disks_io_util(ctxt)
        osds = objects.OsdList.get_all(
            ctxt, expected_attrs=['node', 'disk'])
        if metrics is None:
            return None
        osd_maps = {osd.node.hostname + '-' + osd.disk.name: osd
                    for osd in osds}
        metrics.sort(key=lambda x: x['value'], reverse=True)
        disks = []
        for metric in metrics:
            value = round(float(metric['value']), 2)
            if value == 0:
                continue
            hostname = metric['hostname']
            disk_name = metric['name']
            map_name = hostname + '-' + disk_name
            if map_name in osd_maps:
                disks.append({
                    "hostname": hostname,
                    "osd_name": osd_maps[map_name].osd_name,
                    "disk": disk_name,
                    "value": value
                })
                if len(disks) >= k:
                    break
        logger.info("disk io top(%s): %s", k, disks)
        return disks
