import json
import queue
import time
import uuid
from concurrent import futures

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.action_log import ActionLogHandler
from t2stor.admin.alert_group import AlertGroupHandler
from t2stor.admin.alert_log import AlertLogHandler
from t2stor.admin.ceph_config import CephConfigHandler
from t2stor.admin.datacenter import DatacenterHandler
from t2stor.admin.disk import DiskHandler
from t2stor.admin.email_group import EmailGroupHandler
from t2stor.admin.genconf import ceph_conf
from t2stor.admin.osd import OsdHandler
from t2stor.admin.pool import PoolHandler
from t2stor.admin.prometheus import PrometheusHandler
from t2stor.admin.rack import RackHandler
from t2stor.admin.volume import VolumeHandler
from t2stor.admin.volume_client import VolumeClientHandler
from t2stor.admin.volume_snapshot import VolumeSnapshotHandler
from t2stor.api.wsclient import WebSocketClientManager
from t2stor.objects import fields as s_fields
from t2stor.service import ServiceBase
from t2stor.taskflows.ceph import CephTask
from t2stor.taskflows.node import NodeTask
from t2stor.tools.base import Executor
from t2stor.tools.ceph import CephTool
from t2stor.utils.mail import send_mail

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
OSD_ID_MAX = 1024 ^ 2
logger = logging.getLogger(__name__)
LOCAL_LOGFILE_DIR = '/var/log/t2stor_log/'


class AdminQueue(queue.Queue):
    pass


class AdminHandler(ActionLogHandler,
                   AlertGroupHandler,
                   AlertLogHandler,
                   CephConfigHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   OsdHandler,
                   PoolHandler,
                   PrometheusHandler,
                   RackHandler,
                   VolumeHandler,
                   VolumeClientHandler,
                   VolumeSnapshotHandler):
    def __init__(self):
        self.worker_queue = AdminQueue()
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    def get_ceph_conf(self, ctxt, ceph_host):
        logger.debug("try get ceph conf with location "
                     "{}".format(ceph_host))
        return ceph_conf

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

    def node_get(self, ctxt, node_id, expected_attrs=None):
        node_info = objects.Node.get_by_id(
            ctxt, node_id, expected_attrs=expected_attrs)
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
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        nodes = objects.NodeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
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
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
        networks = objects.NetworkList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs
        )
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

    def crush_rule_create(self, ctxt, rule_name, failure_domain_type,
                          rule_content):
        crush_rule = objects.CrushRule(
            ctxt, cluster_id=ctxt.cluster_id, rule_name=rule_name,
            type=failure_domain_type,
            content=rule_content)
        crush_rule.create()
        return crush_rule

    def crush_rule_get(self, ctxt, crush_rule_id):
        return objects.CrushRule.get_by_id(
            ctxt, crush_rule_id, expected_attrs=['osds'])

    def crush_rule_delete(self, ctxt, crush_rule_id):
        crush_rule = objects.CrushRule.get_by_id(ctxt, crush_rule_id)
        crush_rule.destroy()
        return crush_rule

    ###################

    def volume_access_path_get_all(self, ctxt, marker=None, limit=None,
                                   sort_keys=None, sort_dirs=None,
                                   filters=None, offset=None):
        filters = filters or {}
        return objects.VolumeAccessPathList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_access_path_get(self, ctxt, volume_access_path_id):
        return objects.VolumeAccessPath.get_by_id(ctxt, volume_access_path_id)

    def volume_access_path_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
        })
        v = objects.VolumeAccessPath(
            ctxt, status=s_fields.VolumeAccessPathStatus.CREATING, **data)
        v.create()
        return v

    def volume_access_path_update(self, ctxt, id, data):
        volume_access_path = objects.VolumeAccessPath.get_by_id(ctxt, id)
        volume_access_path.name = data.get("name")
        volume_access_path.save()
        return volume_access_path

    def volume_access_path_delete(self, ctxt, id):
        volume_access_path = objects.VolumeAccessPath.get_by_id(ctxt, id)
        volume_access_path.destroy()
        return volume_access_path

    def _update_osd_crush_id(self, ctxt, osds, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.crush_rule_id = crush_rule_id
            osd.save()

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

    def _volume_create_from_snapshot(self, ctxt, verify_data):
        p_pool_name = verify_data['p_pool_name']
        p_volume_name = verify_data['p_volume_name']
        p_snap_name = verify_data['p_snap_name']
        c_pool_name = verify_data['c_pool_name']
        new_volume = verify_data['new_volume']
        c_volume_name = new_volume.volume_name
        is_link_clone = verify_data['is_link_clone']
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_clone_volume(
                p_pool_name, p_volume_name, p_snap_name, c_pool_name,
                c_volume_name)
            if not is_link_clone:  # 独立克隆，断开关系链
                ceph_client.rbd_flatten(c_pool_name, c_volume_name)
            status = s_fields.VolumeStatus.ACTIVE
            logger.info('volume clone success, volume_name={}'.format(
                c_volume_name))
            msg = 'volume clone success'
        except exception.StorException as e:
            status = s_fields.VolumeStatus.ERROR
            logger.error('volume clone error, volume_name={},reason:{}'.format(
                c_volume_name, str(e)))
            msg = 'volume clone error'
        new_volume.status = status
        new_volume.save()
        # send msg
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=new_volume.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, new_volume, 'VOLUME_CLONE', msg)

    def _verify_clone_data(self, ctxt, snapshot_id, data):
        display_name = data.get('display_name')
        display_description = data.get('display_description')
        c_pool_id = data.get('pool_id')
        is_link_clone = data.get('is_link_clone')
        snap = objects.VolumeSnapshot.get_by_id(ctxt, snapshot_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=snapshot_id)
        snap_name = snap.uuid
        p_volume = objects.Volume.get_by_id(ctxt, snap.volume_id)
        if not p_volume:
            raise exception.VolumeNotFound(volume_id=snap.volume_id)
        size = p_volume.size
        p_volume_name = p_volume.volume_name
        p_pool_name = objects.Pool.get_by_id(ctxt, p_volume.pool_id).pool_name
        c_pool_name = objects.Pool.get_by_id(ctxt, c_pool_id).pool_name
        return {
            'p_pool_name': p_pool_name,
            'p_volume_name': p_volume_name,
            'p_snap_name': snap_name,
            'c_pool_name': c_pool_name,
            'pool_id': c_pool_id,
            'display_name': display_name,
            'size': size,
            'display_description': display_description,
            'is_link_clone': is_link_clone,
        }

    def volume_create_from_snapshot(self, ctxt, snapshot_id, data):
        verify_data = self._verify_clone_data(ctxt, snapshot_id, data)
        # create volume
        uid = str(uuid.uuid4())
        volume_name = "volume-{}".format(uid)
        volume_data = {
            "cluster_id": ctxt.cluster_id,
            'volume_name': volume_name,
            'display_name': verify_data['display_name'],
            'display_description': verify_data['display_description'],
            'size': verify_data['size'],
            'status': s_fields.VolumeStatus.CREATING,
            'snapshot_id': snapshot_id,
            'is_link_clone': verify_data['is_link_clone'],
            'pool_id': verify_data['pool_id']
        }
        new_volume = objects.Volume(ctxt, **volume_data)
        new_volume.create()
        verify_data.update({'new_volume': new_volume})
        # put into thread pool
        self.executor.submit(self._volume_create_from_snapshot, ctxt,
                             verify_data)
        return new_volume

    ###################
    def send_mail(subject, content, config):
        send_mail(subject, content, config)

    def smtp_init(self, ctxt):
        data = [
            ("smtp_enabled", '0', 'string'),
            ("smtp_user", '0', 'string'),
            ("smtp_password", '0', 'string'),
            ("smtp_host", '0', 'string'),
            ("smtp_port", '0', 'string'),
            ("smtp_enable_ssl", 'True', 'string'),
            ("smtp_enable_tls", 'Flase', 'string'),
        ]
        for key, value, value_type in data:
            cfg = objects.SysConfig(key=key, value=value,
                                    value_type=value_type)
            cfg.save()

    def smtp_get(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['smtp_enabled', 'smtp_user', 'smtp_password', 'smtp_host',
                'smtp_port', 'smtp_enable_ssl', 'smtp_enable_tls']
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
        return result

    def update_smtp(self, ctxt, smtp_enabled,
                    smtp_user, smtp_password,
                    smtp_host, smtp_port,
                    smtp_enable_ssl,
                    smtp_enable_tls):
        # TODO check a object exists
        sysconf = None
        if smtp_enabled:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enabled", value=smtp_enabled,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_user:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_user", value=smtp_user,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_password:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_password", value=smtp_password,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_host:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_host", value=smtp_host,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_port:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_port", value=smtp_port,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_enable_ssl:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enable_ssl", value=smtp_enable_ssl,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_enable_tls:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enable_tls", value=smtp_enable_tls,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()

    def ceph_cluster_info(self, ctxt):
        try:
            ceph_client = CephTask(ctxt)
            cluster_info = ceph_client.cluster_info()
        except Exception as e:
            logger.error(e)
            return {}
        fsid = cluster_info.get('fsid')
        total_cluster_byte = cluster_info.get('cluster_data', {}).get(
            'stats', {}).get('total_bytes')
        pool_list = cluster_info.get('cluster_data', {}).get('pools')
        return {
            'fsid': fsid,
            'total_cluster_byte': total_cluster_byte,
            'pool_list': pool_list
        }


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
