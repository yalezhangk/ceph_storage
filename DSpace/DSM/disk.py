import six
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import ConfigKey
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
        guid, partitions = client.disk_partitions_create(
            ctxt, node=node, disk=disk, values=values)
        if len(partitions) and guid:
            partitions_old = objects.DiskPartitionList.get_all(
                ctxt, filters={'disk_id': disk.id})
            if partitions_old:
                for part in partitions_old:
                    part.destroy()

            disk.partition_num = values['partition_num']
            disk.role = values['role']
            disk.guid = guid
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
        disk.accelerate_type = values['partition_role']
        self.finish_action(begin_action, disk.id, disk.name,
                           disk, action_status)
        self.send_websocket(ctxt, disk, op_status, msg)

    def disk_partitions_create(self, ctxt, disk_id, values):
        ceph_version = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.CEPH_VERSION_NAME)
        if (ceph_version == s_fields.CephVersion.T2STOR):
            t2stor_support_type = [
                s_fields.DiskPartitionRole.CACHE,
                s_fields.DiskPartitionRole.WAL,
                s_fields.DiskPartitionRole.JOURNAL
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
        disk = objects.Disk.get_by_id(
                ctxt, disk_id,
                expected_attrs=['partition_used', 'node'])
        if disk.status != s_fields.DiskStatus.AVAILABLE:
            raise exception.InvalidInput(_("Disk status not available"))
        disk.conditional_update({
            "status": s_fields.DiskStatus.PROCESSING
        }, expected_values={
            "status": s_fields.DiskStatus.AVAILABLE
        })
        begin_action = self.begin_action(
            ctxt, Resource.ACCELERATE_DISK, Action.CREATE, disk)
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
        self.send_websocket(ctxt, disk, op_status, msg)

    def disk_can_operation(self, ctxt, disk):
        if disk.status not in disk.can_operation_status:
            return False
        partitions = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk.id})
        for part in partitions:
            if part.status == s_fields.DiskStatus.INUSE:
                return False
        return True

    def disk_partitions_remove(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used'])
        # 分区不为0，不可被删
        if disk.partition_used:
            raise exception.InvalidInput(
                reason=_('current disk:{} partition has used, '
                         'can not del').format(disk.name))
        if not self.disk_can_operation(ctxt, disk):
            raise exception.InvalidInput(_("Disk status not available"))
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        begin_action = self.begin_action(
            ctxt, Resource.ACCELERATE_DISK, Action.DELETE, disk)
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

    def disk_offline(self, ctxt, disk_name, node_id):
        logger.info("recieve disk offline on %s: %s", node_id, disk_name)
        disk = objects.Disk.get_by_name(
            ctxt, disk_name, node_id,
            expected_attrs=['node', 'partition_used', 'partitions'])
        if not disk:
            logger.info('No disk found, name: %s', disk_name)
            return
        # send websocket
        msg = (_('node %s: disk %s offline') % (disk.node.hostname, disk.name))
        self.send_websocket(ctxt, disk, 'DISK_OFFLINE', msg)
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
                    'alert_value': msg,
                    'alert_rule_id': alert_rule.id,
                    'cluster_id': ctxt.cluster_id
                }
                alert_log = objects.AlertLog(ctxt, **alert_log_data)
                alert_log.create()
                logger.info('alert %s happen: %s', alert_log.resource_type,
                            alert_log.alert_value)
        disk.name = None
        disk.save()
        if disk.status in s_fields.DiskStatus.REPLACE_STATUS:
            logger.info("%s in replacing task, do nothing", disk.name)
            return
        if disk.role == s_fields.DiskRole.ACCELERATE:
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
                partition.status = s_fields.DiskStatus.ERROR
                partition.save()
            disk.status = s_fields.DiskStatus.ERROR
            disk.save()
        elif disk.role == s_fields.DiskRole.DATA:
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
                disk.status = s_fields.DiskStatus.ERROR
                disk.save()
            else:
                logger.info('disk %s not used, remove it', disk.name)
                disk.destroy()
            logger.info('disk %s pull out, mark it error', disk.name)
        else:
            logger.error("Disk role type error")

    def _disk_add_new(self, ctxt, data, node_id):
        partitions = data.get('partitions')
        name = data.get('name')
        logger.info("Create node_id %s disk %s: %s",
                    node_id, name, data)
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
            name=name,
            status=status,
            type=data.get('type', s_fields.DiskType.HDD),
            size=data.get('size'),
            rotate_speed=data.get('rotate_speed'),
            slot=data.get('slot'),
            serial=data.get('serial'),
            wwid=data.get('wwid'),
            guid=data.get('guid'),
            node_id=node_id,
            partition_num=len(partitions),
            role=role
        )
        disk.create()
        return disk

    def _disk_update_accelerate(self, ctxt, disk,
                                disk_info, node_id, init=False):
        disk_name = disk_info.get('name')
        logger.info('update accelerate disk: %s', disk_name)
        if disk.status in s_fields.DiskStatus.REPLACE_STATUS:
            logger.info("%s in replacing task, do nothing", disk_name)
        else:
            # check disk part table
            partitions_obj = objects.DiskPartitionList.get_all(
                ctxt, filters={'disk_id': disk.id})
            parts_with_uuid = {
                part.uuid: part for part in partitions_obj
            }
            new_partitions = disk_info.get('partitions')
            disk_status = s_fields.DiskStatus.INUSE
            for new_part in new_partitions:
                if new_part.get('uuid') not in parts_with_uuid:
                    continue
                part = parts_with_uuid.pop(new_part.get('uuid'))
                osd = self._osd_get_by_accelerate_partition(
                    ctxt, part, expected_attrs=['node'])
                if not osd:
                    part.status = s_fields.DiskStatus.AVAILABLE
                    disk_status = s_fields.DiskStatus.AVAILABLE
                elif osd.status in s_fields.OsdStatus.REPLACE_STATUS or init:
                    part.status = s_fields.DiskStatus.INUSE
                else:
                    logger.info('accelerate disk has osd.%s, restarting',
                                osd.osd_id)
                    task = NodeTask(ctxt, osd.node)
                    task.ceph_osd_restart(osd)
                    osd.status = s_fields.OsdStatus.ACTIVE
                    osd.save()
                    part.status = s_fields.DiskStatus.INUSE
                part.name = new_part.get('name')
                part.save()
            disk.status = disk_status
            for uuid, part in six.iteritems(parts_with_uuid):
                logger.error("disk partition uuid %s not found", uuid)
                osd = self._osd_get_by_accelerate_partition(ctxt, part)
                if osd:
                    part.status = s_fields.DiskStatus.INUSE
                else:
                    part.status = s_fields.DiskStatus.ERROR
                part.save()
                disk.status = s_fields.DiskStatus.ERROR
        disk.type = s_fields.DiskType.SSD
        disk.save()

    def _disk_update_data(self, ctxt, disk, disk_info, node_id):
        logger.info('update data disk: %s', disk.name)
        osds = objects.OsdList.get_all(
            ctxt, filters={'disk_id': disk.id}, expected_attrs=['node'])
        if disk.status in s_fields.DiskStatus.REPLACE_STATUS:
            logger.info("%s in replacing task, do nothing", disk.name)
        elif len(osds):
            logger.info('disk %s has osd, set status to inuse', disk.name)
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
        disk = objects.Disk.get_by_guid(
            ctxt, disk_info.get('guid'), node_id,
            expected_attrs=['node', 'partition_used', 'partitions'])

        if not disk:
            disk = self._disk_add_new(ctxt, disk_info, node_id)
            disk = objects.Disk.get_by_id(
                ctxt, disk.id, expected_attrs=['partition_used', 'node',
                                               'partitions'])
        else:
            disk.name = disk_info.get('name')
            disk.slot = disk_info.get('slot')
            disk.wwid = disk_info.get('wwid')
            disk.serial = disk_info.get('serial')
            disk.type = disk_info.get('type')
            disk.size = int(disk_info.get('size'))
            disk.save()
            if disk.role == s_fields.DiskRole.ACCELERATE:
                self._disk_update_accelerate(ctxt, disk, disk_info, node_id)
            elif disk.role == s_fields.DiskRole.DATA:
                self._disk_update_data(ctxt, disk, disk_info, node_id)
            else:
                logger.error("Disk role type error")
        # send websocket
        msg = (_('node %s: disk %s online') % (disk.node.hostname, disk.name))
        self.send_websocket(ctxt, disk, 'DISK_ONLINE', msg)
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
                    'alert_value': msg,
                    'alert_rule_id': alert_rule.id,
                    'cluster_id': ctxt.cluster_id
                }
                alert_log = objects.AlertLog(ctxt, **alert_log_data)
                alert_log.create()
                logger.info('alert %s happen: %s', alert_log.resource_type,
                            alert_log.alert_value)

    def disk_partitions_reporter(self, ctxt, partitions, disk):
        partitions_objs = objects.DiskPartitionList.get_all(
            ctxt, filters={'disk_id': disk.id})
        parts_with_uuid = {
            part.uuid: part for part in partitions_objs
        }
        for new_part in partitions:
            if new_part.get('uuid') in parts_with_uuid:
                part = parts_with_uuid.pop(new_part.get('uuid'))
                part.name = new_part.get('name')
                part.size = new_part.get('size')
                part.save()
            else:
                part = objects.DiskPartition(
                    ctxt,
                    name=new_part.get('name'),
                    size=new_part.get('size'),
                    status=s_fields.DiskStatus.AVAILABLE,
                    type=new_part.get('type', s_fields.DiskType.HDD),
                    node_id=disk.node_id,
                    disk_id=disk.id,
                )
                part.create()
        for uuid, part in six.iteritems(parts_with_uuid):
            logger.warning("Remove partition %s", part.name)
            part.destroy()

    def disk_reporter(self, ctxt, disks, node_id):
        logger.info("get disks report: %s", disks)
        all_disk_objs = objects.DiskList.get_all(
            ctxt, filters={'node_id': node_id},
            expected_attrs=['partition_used']
        )
        disks_with_guid = {
            disk.guid: disk for disk in all_disk_objs if disk.guid
        }
        disks_without_guid = [
            disk for disk in all_disk_objs if not disk.guid
        ]
        for name, data in six.iteritems(disks):
            logger.info("Check node_id %s disk %s: %s",
                        node_id, name, data)
            partitions = data.get("partitions")
            if data.get("guid") in disks_with_guid:
                disk = disks_with_guid.pop(data.get('guid'))
                disk.name = name
                disk.slot = data.get('slot')
                disk.wwid = data.get('wwid')
                disk.serial = data.get('serial')
                disk.size = int(data.get('size'))
                disk.save()
                # disk check
                if disk.role == s_fields.DiskRole.DATA:
                    self._disk_update_data(
                        ctxt, disk, data, node_id)
                    # check osd disk part uuid
                elif disk.role == s_fields.DiskRole.ACCELERATE:
                    # check ACCELERATE partition
                    self._disk_update_accelerate(
                        ctxt, disk, data, node_id, init=True)
                else:
                    logger.debug("disk %s is system disk", disk.name)
                    disk.status = s_fields.DiskStatus.INUSE
                    disk.save()
                logger.info("Update node_id %s disk %s: %s",
                            node_id, name, data)
            else:
                disk = self._disk_add_new(ctxt, data, node_id)
                self.disk_partitions_reporter(ctxt, partitions, disk)

        for guid, disk in six.iteritems(disks_with_guid):
            osds = objects.OsdList.get_all(
                ctxt, filters={'disk_id': disk.id})
            if osds:
                disk.status = s_fields.DiskStatus.ERROR
                disk.save()
            elif disk.role == s_fields.DiskRole.ACCELERATE:
                partitions = objects.DiskPartitionList.get_all(
                    ctxt, filters={'disk_id': disk.id})
                for part in partitions:
                    osd = self._osd_get_by_accelerate_partition(ctxt, part)
                    if osd:
                        part.status = s_fields.DiskStatus.INUSE
                    else:
                        part.status = s_fields.DiskStatus.ERROR
                    part.save()
                disk.status = s_fields.DiskStatus.ERROR
                disk.save()
            else:
                disk.destroy()
        # remove none guid disk
        for disk in disks_without_guid:
            logger.info("Remove none guid disk %s", disk.id)
            disk.destroy()

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
        osd_maps = {}
        for osd in osds:
            if osd.node.hostname and osd.disk.name:
                osd_maps[osd.node.hostname + '-' + osd.disk.name] = osd
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
