import six
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
                                         Action.CHANGE_DISK_TYPE)
        disk.type = disk_type
        disk.save()
        self.finish_action(begin_action, disk_id, disk.name,
                           objects.json_encode(disk))
        return disk

    def disk_light(self, ctxt, disk_id, led):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if disk.support_led:
            if disk.led == led:
                raise exception.InvalidInput(
                    reason=_("disk: repeated actions, led is {}".format(led)))
            node = objects.Node.get_by_id(ctxt, disk.node_id)
            begin_action = self.begin_action(ctxt, Resource.DISK,
                                             Action.DISK_LIGHT)
            client = AgentClientManager(
                ctxt, cluster_id=disk.cluster_id
            ).get_client(node_id=disk.node_id)
            _success = client.disk_light(ctxt, led=led, node=node,
                                         name=disk.name)
            if _success:
                disk.led = led
                disk.save()
                status = 'success'
            else:
                status = 'fail'
            self.finish_action(begin_action, disk_id, disk.name,
                               objects.json_encode(disk), status)
        else:
            raise exception.LedNotSupport(disk_id=disk_id)
        return disk

    def _disk_partitions_create(self, ctxt, node, disk, values,
                                begin_action=None):
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client(node_id=disk.node_id)
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
                    status="available", type=disk.type,
                    role=part.get('role'), node_id=disk.node_id,
                    disk_id=disk.id, cluster_id=disk.cluster_id,
                )
                partition.create()
            disk.save()
            msg = _("create disk partitions success")
            status = 'success'
        else:
            msg = _("create disk partitions failed")
            status = 'fail'
        self.finish_action(begin_action, disk.id, disk.name,
                           objects.json_encode(disk), status)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, "CREATED", msg)

    def disk_partitions_create(self, ctxt, disk_id, values):
        begin_action = self.begin_action(ctxt, Resource.DISK, Action.CREATE)
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        self.task_submit(self._disk_partitions_create, ctxt, node, disk,
                         values, begin_action)
        return disk

    def _disk_partitions_remove(self, ctxt, node, disk, values,
                                begin_action=None):
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client(node_id=disk.node_id)
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
            disk.status = status
            disk.save()
            msg = _("remove disk partitions success")
        else:
            logger.error("Disk partitions remove: Failed")
            msg = _("remove disk partitions failed")
            status = 'fail'
        self.finish_action(begin_action, disk.id, disk.name,
                           objects.json_encode(disk), status)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, disk, "REMOVED", msg)

    def disk_partitions_remove(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(
            ctxt, disk_id, expected_attrs=['partition_used'])
        # 分区不为0，不可被删
        if disk.partition_used:
            raise exception.InvalidInput(
                reason=_('current disk:{} partition has used, '
                         'can not del').format(disk.name))
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        begin_action = self.begin_action(ctxt, Resource.DISK, Action.DELETE)
        self.task_submit(self._disk_partitions_remove, ctxt, node, disk,
                         values, begin_action)
        return disk

    def disk_smart_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id).get_client(node_id=disk.node_id)
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

    def disk_reporter(self, ctxt, disks, node_id):
        all_disk_objs = objects.DiskList.get_all(
            ctxt, filters={'node_id': node_id},
            expected_attrs=['partition_used']
        )
        all_disks = {
            disk.name: disk for disk in all_disk_objs
        }
        for name, data in six.iteritems(disks):
            logger.info("Check node_id %s disk %s: %s", node_id, name, data)
            partitions = data.get("partitions")
            if name in all_disks:
                disk = all_disks.pop(name)
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
                disk.save()
                logger.info("Update node_id %s disk %s: %s",
                            node_id, name, data)
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
                    name=name,
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
                            node_id, name, data)
            # TODO don't use now
            self.disk_partitions_reporter(ctxt, partitions, disk)
        for name, disk in six.iteritems(all_disks):
            logger.warning("Remove node_id %s, disk %s", node_id, name)
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
