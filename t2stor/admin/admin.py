import json
import queue
import time
import uuid
from concurrent import futures

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.genconf import ceph_conf
from t2stor.agent.client import AgentClientManager
from t2stor.i18n import _
from t2stor.objects import fields as s_fields
from t2stor.service import ServiceBase
from t2stor.taskflows.node import NodeTask
from t2stor.tools.base import Executor
from t2stor.tools.ceph import Ceph as CephTool

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AdminQueue(queue.Queue):
    pass


class AdminHandler(object):
    def __init__(self):
        self.worker_queue = AdminQueue()
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    def get_ceph_conf(self, ctxt, ceph_host):
        logger.debug("try get ceph conf with location "
                     "{}".format(ceph_host))
        return ceph_conf

    ###################

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.VolumeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_get(self, ctxt, volume_id):
        return objects.Volume.get_by_id(ctxt, volume_id)

    def volume_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
            'status': 'creating',
            'volume_name': 'V',  # TODO ceph volume name
        })
        v = objects.Volume(ctxt, **data)
        v.create()
        # TODO create volume
        return v

    def volume_update(self, ctxt, volume_id, data):
        volume = self.volume_get(ctxt, volume_id)
        for k, v in six.iteritems(data):
            setattr(volume, k, v)
        volume.save()
        return volume

    def volume_delete(self, ctxt, volume_id):
        volume = self.volume_get(ctxt, volume_id)
        volume.destroy()
        # TODO delete ceph volume
        return volume

    def volume_extend(self, ctxt, volume_id, data):
        volume = self.volume_update(ctxt, volume_id, data)
        # TODO ceph volume_extend
        return volume

    def volume_shrink(self, ctxt, volume_id, data):
        volume = self.volume_update(ctxt, volume_id, data)
        # TODO ceph volume_shrink
        return volume

    def volume_rollback(self, ctxt, volume_id, data):
        pass
        # TODO ceph volume_rollback

    def volume_unlink(self, ctxt, volume_id, data):
        pass
        # TODO ceph volume_unlink

    ###################

    def cluster_import(self, ctxt):
        """Cluster import"""
        pass

    def cluster_new(self, ctxt):
        """Deploy a new cluster"""
        pass

    def cluster_get_info(self, ctxt, ip_address, password=None):
        logger.debug("detect an exist cluster from {}".format(ip_address))
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        tool = CephTool(ssh_client)
        cluster_info = {}
        mon_hosts = tool.get_mons()
        osd_hosts = tool.get_osds()
        mgr_hosts = tool.get_mgrs()
        cluster_network, public_network = tool.get_networks()

        cluster_info.update({'mon_hosts': mon_hosts,
                             'osd_hosts': osd_hosts,
                             'mgr_hosts': mgr_hosts,
                             'public_network': str(public_network),
                             'cluster_network': str(cluster_network)})
        return cluster_info

    def cluster_install_agent(self, ctxt, ip_address, password):
        logger.debug("Install agent on {}".format(ip_address))
        task = NodeTask()
        task.t2stor_agent_install(ip_address, password)
        return True

    def alert_rule_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.AlertRuleList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_rule_get(self, ctxt, alert_rule_id):
        return objects.AlertRule.get_by_id(ctxt, alert_rule_id)

    def alert_rule_update(self, ctxt, alert_rule_id, data):
        rule = self.alert_rule_get(ctxt, alert_rule_id)
        for k, v in six.iteritems(data):
            setattr(rule, k, v)

        rule.save()
        return rule

    def node_get(self, ctxt, node_id):
        node_info = objects.Node.get_by_id(ctxt, node_id)
        return node_info

    def node_update(self, ctxt, node_id, data):
        node = objects.Node.get_by_id(ctxt, node_id)
        for k, v in six.iteritems(data):
            setattr(node, k, v)
        node.save()
        return node

    def node_delete(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        node.destroy()
        return node

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        nodes = objects.NodeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return nodes

    def node_create(self, ctxt, data):
        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            password=data.get('password'),
            gateway_ip_address=data.get('gateway_ip_address'),
            storage_cluster_ip_address=data.get('storage_cluster_ip_address'),
            storage_public_ip_address=data.get('storage_public_ip_address'),
            status='creating')
        node.create()
        return node

    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
        networks = objects.NetworkList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return networks

    def sysconf_get_all(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['cluster_name', 'admin_cidr', 'public_cidr', 'cluster_cidr',
                'gateway_cidr', 'chrony_server']
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
        return result

    def update_chrony(self, ctxt, chrony_server):
        sysconf = objects.SysConfig(
            ctxt, key="chrony_server", value=chrony_server,
            value_type=s_fields.SysConfigType.STRING)
        sysconf.create()

    def update_sysinfo(self, ctxt, cluster_name, admin_cidr, public_cidr,
                       cluster_cidr, gateway_cidr):
        # TODO check a object exists
        sysconf = None
        if cluster_name:
            sysconf = objects.SysConfig(
                ctxt, key="cluster_name", value=cluster_name,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if admin_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="admin_cidr", value=admin_cidr,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if public_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="public_cidr", value=public_cidr,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if cluster_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="cluster_cidr", value=cluster_cidr,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if gateway_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="gateway_cidr", value=gateway_cidr,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()

    ###################

    def datacenter_create(self, ctxt):
        uid = str(uuid.uuid4())
        datacenter_name = "datacenter-{}".format(uid[0:8])
        datacenter = objects.Datacenter(
            ctxt, cluster_id=ctxt.cluster_id,
            name=datacenter_name
        )
        datacenter.create()
        return datacenter

    def datacenter_get(self, ctxt, datacenter_id):
        return objects.Datacenter.get_by_id(ctxt, datacenter_id)

    def datacenter_delete(self, ctxt, datacenter_id):
        datacenter = self.datacenter_get(ctxt, datacenter_id)
        datacenter.destroy()
        return datacenter

    def datacenter_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.DatacenterList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def datacenter_update(self, ctxt, id, name):
        datacenter = objects.Datacenter.get_by_id(ctxt, id)
        datacenter.name = name
        datacenter.save()
        return datacenter

    def datacenter_racks(self, ctxt, datacenter_id):
        filters = {}
        filters['datacenter_id'] = datacenter_id
        racks = self.rack_get_all(ctxt, filters=filters)
        return racks

    ###################

    def rack_create(self, ctxt, datacenter_id):
        uid = str(uuid.uuid4())
        rack_name = "rack-{}".format(uid[0:8])
        rack = objects.Rack(
            ctxt, cluster_id=ctxt.cluster_id,
            datacenter_id=datacenter_id,
            name=rack_name
        )
        rack.create()
        return rack

    def rack_get(self, ctxt, rack_id):
        return objects.Rack.get_by_id(ctxt, rack_id)

    def rack_delete(self, ctxt, rack_id):
        rack = self.rack_get(ctxt, rack_id)
        rack.destroy()
        return rack

    def rack_get_all(self, ctxt, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.RackList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def rack_update_name(self, ctxt, id, name):
        rack = objects.Rack.get_by_id(ctxt, id)
        rack.name = name
        rack.save()
        return rack

    def rack_have_osds(self, ctxt, rack_id):
        # TODO 检查机架中的OSD是否在一个存储池中
        pass

    def rack_update_toplogy(self, ctxt, id, datacenter_id):
        rack = objects.Rack.get_by_id(ctxt, id)
        if self.rack_have_osds(ctxt, id):
            return rack
        rack.datacenter_id = datacenter_id
        rack.save()
        return rack

    ###################

    def disk_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        return disk

    def disk_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        disks = objects.DiskList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return disks

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
                    reason=_("disk: repeating actions, led is {}".format(led)))
            # TODO: Sending to agent to change led status
            disk.led = 'on' if disk.led == "off" else "off"
            disk.save()
        else:
            raise exception.LedNotSupport(disk_id=disk_id)
        return disk

    def disk_cache(self, ctxt, disk_id, values):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        # TODO : Sending request to agent to make partitions
        disk.partition_num = values['partition_num']
        disk.role = values['role']
        disk.save()
        return disk

    def disk_smart_get(self, ctxt, disk_id):
        disk = objects.Disk.get_by_id(ctxt, disk_id)
        client = AgentClientManager(
            ctxt, cluster_id=disk.cluster_id).get_client()
        smart = client.disk_smart_get(ctxt, name=disk.name)
        return smart

    def disk_partition_get_all(self, ctxt, marker=None, limit=None,
                               sort_keys=None, sort_dirs=None, filters=None,
                               offset=None):
        disks = objects.DiskPartitionList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return disks

    ###################

    def email_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.EmailGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def email_group_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        emai_group = objects.EmailGroup(ctxt, **data)
        emai_group.create()
        return emai_group

    def email_group_get(self, ctxt, email_group_id):
        return objects.EmailGroup.get_by_id(ctxt, email_group_id)

    def email_group_update(self, ctxt, email_group_id, data):
        email_group = self.email_group_get(ctxt, email_group_id)
        for k, v in six.iteritems(data):
            setattr(email_group, k, v)
        email_group.save()
        return email_group

    def email_group_delete(self, ctxt, email_group_id):
        email_group = self.email_group_get(ctxt, email_group_id)
        email_group.destroy()
        return email_group

    ###################

    def alert_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.AlertGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_group_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_group = objects.AlertGroup(ctxt, **data)
        alert_group.create()
        return alert_group

    def alert_group_get(self, ctxt, alert_group_id):
        return objects.AlertGroup.get_by_id(ctxt, alert_group_id)

    def alert_group_update(self, ctxt, alert_group_id, data):
        alert_group = self.alert_group_get(ctxt, alert_group_id)
        for k, v in six.iteritems(data):
            setattr(alert_group, k, v)
        alert_group.save()
        return alert_group

    def alert_group_delete(self, ctxt, alert_group_id):
        alert_group = self.alert_group_get(ctxt, alert_group_id)
        alert_group.destroy()
        return alert_group

    ###################

    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        return objects.PoolList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def pool_get(self, ctxt, pool_id):
        return objects.Pool.get_by_id(ctxt, pool_id)

    def pool_osds_get(self, ctxt, pool_id):
        return objects.OsdList.get_by_pool(ctxt, pool_id)

    ###################

    def alert_log_get_all(self, ctxt, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None, filters=None,
                          offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.AlertLogList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_log_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_log = objects.AlertLog(ctxt, **data)
        alert_log.create()
        # TODO send email
        # 根据alert_rule,alert_group,alert_email发送邮件
        return alert_log

    def alert_log_get(self, ctxt, alert_log_id):
        return objects.AlertLog.get_by_id(ctxt, alert_log_id)

    def alert_log_update(self, ctxt, alert_log_id, data):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        for k, v in six.iteritems(data):
            setattr(alert_log, k, v)
        alert_log.save()
        return alert_log

    def alert_log_delete(self, ctxt, alert_log_id):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        alert_log.destroy()
        return alert_log

    ###################

    def log_file_get_all(self, ctxt, marker=None, limit=None,
                         sort_keys=None, sort_dirs=None, filters=None,
                         offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.LogFileList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def log_file_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_log = objects.LogFile(ctxt, **data)
        alert_log.create()
        return alert_log

    def log_file_get(self, ctxt, log_file_id):
        return objects.LogFile.get_by_id(ctxt, log_file_id)

    def log_file_update(self, ctxt, log_file_id, data):
        log_file = self.log_file_get(ctxt, log_file_id)
        for k, v in six.iteritems(data):
            setattr(log_file, k, v)
        log_file.save()
        return log_file

    def log_file_delete(self, ctxt, log_file_id):
        log_file = self.log_file_get(ctxt, log_file_id)
        log_file.destroy()
        return log_file

    ###################

    def services_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                         sort_dirs=None, filters=None, offset=None):
        services = objects.ServiceList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return services

    def service_update(self, ctxt, services):
        services = json.loads(services)
        logger.debug('Update service status')
        for s in services:
            filters = {
                "name": s['name'],
                "node_id": s['node_id']
            }
            service = objects.ServiceList.get_all(ctxt, filters=filters)
            if not service:
                service_new = objects.Service(
                    ctxt, name=s.get('name'), status=s.get('status'),
                    node_id=s.get('node_id'), cluster_id=ctxt.cluster_id
                )
                service_new.create()
            else:
                service = service[0]
                service.status = s.get('status')
                service.save()
        return True

    def volume_snapshot_get_all(self, ctxt, marker=None, limit=None,
                                sort_keys=None, sort_dirs=None, filters=None,
                                offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
        return objects.VolumeSnapshotList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_snapshot_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
            'uuid': 'S',  # TODO ceph_create snapshot
            'status': 'creating'
        })
        alert_log = objects.VolumeSnapshot(ctxt, **data)
        alert_log.create()
        # TODO create snapshot
        return alert_log

    def volume_snapshot_get(self, ctxt, volume_snapshot_id):
        return objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id)

    def volume_snapshot_update(self, ctxt, volume_snapshot_id, data):
        volume_snapshot = self.volume_snapshot_get(ctxt, volume_snapshot_id)
        for k, v in six.iteritems(data):
            setattr(volume_snapshot, k, v)
        volume_snapshot.save()
        return volume_snapshot

    def volume_snapshot_delete(self, ctxt, volume_snapshot_id):
        volume_snapshot = self.volume_snapshot_get(ctxt, volume_snapshot_id)
        volume_snapshot.destroy()
        return volume_snapshot

    def volume_create_from_snapshot(self, ctxt, volume_snapshot_id, data):
        volume_snapshot = objects.VolumeSnapshot.get_by_id(
            ctxt, volume_snapshot_id)
        size = objects.Volume.get_by_id(ctxt, volume_snapshot.volume_id).size
        data.update({'size': size, 'snapshot_id': volume_snapshot_id})
        new_volume = self.volume_create(ctxt, data)
        # TODO ceph_clone
        return new_volume

    ###################

    def smtp_get(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['enabled', 'smtp_user', 'smtp_password', 'smtp_host',
                'smtp_port', 'enable_ssl', 'enable_tls']
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
        return result

    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        ceph_conf = objects.CephConfigList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return ceph_conf

    def ceph_config_set(self, ctxt, values):
        filters = {
            "group": values['group'],
            "key": values['key']
        }
        cephconf = objects.CephConfigList.get_all(ctxt, filters=filters)
        if not cephconf:
            cephconf = objects.CephConfig(
                ctxt, group=values.get('group'), key=values.get('key'),
                value=values.get('value'),
                display_description=values.get('display_description'),
                cluster_id=ctxt.cluster_id
            )
            cephconf.create()
        else:
            cephconf = cephconf[0]
            cephconf.value = values.get('value')
            cephconf.save()
        return cephconf


class AdminService(ServiceBase):
    service_name = "admin"

    def __init__(self):
        self.handler = AdminHandler()
        super(AdminService, self).__init__()


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


def service():
    admin = AdminService()
    admin.start()
    run_loop()
    admin.stop()
