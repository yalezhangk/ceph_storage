from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class DiskHandler(AdminBaseHandler):
    def disk_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id,
                                      expected_attrs=['partition_used'])
        return disk

    def disk_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        disks = objects.DiskList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=['node', 'partition_used'])
        return disks

    def disk_get_count(self, ctxt, filters=None):
        return objects.DiskList.get_count(ctxt, filters=filters)

    def disk_update(self, ctxt, disk_id, disk_type):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        disk.type = disk_type
        disk.save()
        return disk

    def disk_light(self, ctxt, disk_id, led):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        if disk.support_led:
            if disk.led == led:
                raise exception.InvalidInput(
                    reason=_("disk: repeated actions, led is {}".format(led)))
            node = objects.Node.get_by_id(ctxt, disk.node_id)
            client = AgentClientManager(
                ctxt, cluster_id=disk.cluster_id
            ).get_client(node_id=disk.node_id)
            _success = client.disk_light(ctxt, led=led, node=node,
                                         name=disk.name)
            if _success:
                disk.led = led
                disk.save()
        else:
            raise exception.LedNotSupport(disk_id=disk_id)
        return disk

    def _disk_partitions_create(self, ctxt, node, disk, values):
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
        else:
            msg = _("create disk partitions failed")

        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, disk, "CREATED", msg)

    def disk_partitions_create(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        self.executor.submit(
            self._disk_partitions_create(ctxt, node, disk, values))
        return disk

    def _disk_partitions_remove(self, ctxt, node, disk, values):
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client(node_id=disk.node_id)
        _success = client.disk_partitions_remove(ctxt, node=node,
                                                 name=disk.name, )
        if _success:
            partitions_old = objects.DiskPartitionList.get_all(
                ctxt, filters={'disk_id': disk.id})
            if partitions_old:
                for part in partitions_old:
                    part.destroy()
            disk.partition_num = 0
            disk.role = values['role']
            disk.status = "available"
            msg = _("remove disk partitions success")
        else:
            msg = _("remove disk partitions failed")
        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=disk.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, disk, "REMOVED", msg)

    def disk_partitions_remove(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        node = objects.Node.get_by_id(ctxt, disk.node_id)
        self.executor.submit(
            self._disk_partitions_remove(ctxt, node, disk, values))
        return disk

    def disk_smart_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id).get_client()
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
