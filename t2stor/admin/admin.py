import json
import os
import queue
import time
import uuid
from concurrent import futures

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.action_log import ActionLogHandler
from t2stor.admin.alert_log import AlertLogHandler
from t2stor.admin.datacenter import DatacenterHandler
from t2stor.admin.disk import DiskHandler
from t2stor.admin.email_group import EmailGroupHandler
from t2stor.admin.genconf import ceph_conf
from t2stor.admin.osd import OsdHandler
from t2stor.admin.pool import PoolHandler
from t2stor.admin.rack import RackHandler
from t2stor.admin.volume import VolumeHandler
from t2stor.agent.client import AgentClientManager
from t2stor.api.wsclient import WebSocketClientManager
from t2stor.i18n import _
from t2stor.objects import fields as s_fields
from t2stor.service import ServiceBase
from t2stor.taskflows.ceph import CephTask
from t2stor.taskflows.node import NodeTask
from t2stor.tools.base import Executor
from t2stor.tools.ceph import CephTool
from t2stor.tools.prometheus import PrometheusTool
from t2stor.utils import cluster_config as ClusterConfg
from t2stor.utils.mail import send_mail

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
OSD_ID_MAX = 1024 ^ 2
logger = logging.getLogger(__name__)
LOCAL_LOGFILE_DIR = '/var/log/t2stor_log/'


class AdminQueue(queue.Queue):
    pass


class AdminHandler(ActionLogHandler,
                   AlertLogHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   OsdHandler,
                   PoolHandler,
                   RackHandler,
                   VolumeHandler):
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

    def volume_client_group_get_all(self, ctxt, marker=None, limit=None,
                                    sort_keys=None, sort_dirs=None,
                                    filters=None, offset=None):
        filters = filters or {}
        return objects.VolumeClientGroupList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_client_group_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
        })
        v = objects.VolumeClientGroup(ctxt, **data)
        v.create()
        return v

    def volume_client_create(self, ctxt, data):
        data.update({
            'cluster_id': ctxt.cluster_id,
        })
        v = objects.VolumeClient(ctxt, **data)
        v.create()
        return v

    def volume_client_group_get(self, ctxt, group_id):
        return objects.VolumeClientGroup.get_by_id(ctxt, group_id)

    def volume_client_group_delete(self, ctxt, group_id):
        filters = {"volume_client_group_id": group_id}
        # delete volume clients of the volume client group
        for vc in objects.VolumeClientList.get_all(ctxt, filters=filters):
            vc.destroy()
        volume_client_group = objects.VolumeClientGroup.get_by_id(
            ctxt, group_id)
        volume_client_group.destroy()
        return volume_client_group

    def volume_client_get_all(self, ctxt, marker=None, limit=None,
                              sort_keys=None, sort_dirs=None,
                              filters=None, offset=None):
        filters = filters or {}
        return objects.VolumeClientList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    ###################

    def log_file_get_all(self, ctxt, node_id, service_type, marker=None,
                         limit=None, sort_keys=None, sort_dirs=None,
                         filters=None, offset=None):
        # 1 参数校验，node/osd是否存在
        node = objects.Node.get_by_id(ctxt, node_id)
        if not node:
            raise exception.NodeNotFound(node_id=node_id)
        if service_type == s_fields.LogfileType.MON:
            if not node.role_monitor:
                raise exception.InvalidInput(
                    reason=_('node_id {} is not monitor role'.format(node_id)))
        elif service_type == s_fields.LogfileType.OSD:
            if not node.role_storage:
                raise exception.InvalidInput(
                    reason=_('node_id {} is not storage role'.format(node_id)))
        else:
            raise exception.InvalidInput(reason=_('service_type must is mon or'
                                                  'osd'))
        # 2 agent获取日志文件元数据
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node_id=int(node_id))
        metadata = client.get_logfile_metadata(
            ctxt, node=node, service_type=service_type)
        if not metadata:
            logger.error('get log_file metadata erro or without log_files')
            return None
        # 3 入库，删除旧的，再新增
        logger.info('get log_file metadata success')
        del_filter = {'cluster_id': ctxt.cluster_id, 'node_id': node_id,
                      'service_type': service_type}
        del_objs = objects.LogFileList.get_all(ctxt, filters=del_filter)
        for del_obj in del_objs:
            del_obj.destroy()
        result = []
        for per_log in metadata:
            filename = per_log['file_name']
            filesize = per_log['file_size']
            directory = per_log['directory']
            filters = {'cluster_id': ctxt.cluster_id, 'node_id': node_id,
                       'service_type': service_type, 'filename': filename,
                       'filesize': filesize, 'directory': directory}
            new_log = objects.LogFile(ctxt, **filters)
            new_log.create()
            result.append(new_log)
        # todo 分页返回
        return result

    def log_file_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_log = objects.LogFile(ctxt, **data)
        alert_log.create()
        return alert_log

    def log_file_get(self, ctxt, log_file_id):
        # todo
        # 参数校验
        log_file = objects.LogFile.get_by_id(ctxt, log_file_id)
        if not log_file:
            raise exception.LogFileNotFound(log_file_id=log_file_id)
        node = objects.Node.get_by_id(ctxt, log_file.node_id)
        directory = log_file.directory
        filename = log_file.filename
        # 拉取agent上文件到本机文件夹下
        if not os.path.exists(LOCAL_LOGFILE_DIR):
            os.makedirs(LOCAL_LOGFILE_DIR, mode=0o0755)
        try:
            task = NodeTask(ctxt, node)
            task.pull_logfile(directory, filename, LOCAL_LOGFILE_DIR)
        except exception.StorException as e:
            logger.error('pull log_file error,{}'.format(e))
            raise exception.CephException(message='pull log_file error')
        return '{}{}'.format(LOCAL_LOGFILE_DIR, filename)

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
        volume_id = data.get('volume_id')
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        data.update({'volume_name': volume.volume_name,
                     'pool_name': pool.pool_name})
        uid = str(uuid.uuid4())
        data.update({
            'cluster_id': ctxt.cluster_id,
            'uuid': uid,
            'status': s_fields.VolumeSnapshotStatus.CREATING
        })
        extra_data = {'volume_name': data.pop('volume_name'),
                      'pool_name': data.pop('pool_name')}
        snap = objects.VolumeSnapshot(ctxt, **data)
        snap.create()
        # TODO create snapshot
        self.executor.submit(self._snap_create, ctxt, snap, extra_data)
        return snap

    def _snap_create(self, ctxt, snap, extra_data):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.rbd_snap_create(pool_name, volume_name, snap_name)
            # 新创建的快照均开启快照保护
            ceph_client.rbd_protect_snap(pool_name, volume_name, snap_name)
            status = s_fields.VolumeSnapshotStatus.ACTIVE
            logger.info('create snapshot success,{}/{}@{}'.format(
                pool_name, volume_name, snap_name))
            msg = 'volume_snapshot create success'
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            logger.error('create snapshot error,{}/{}@{},reason:{}'.format(
                pool_name, volume_name, snap_name, str(e)))
            msg = 'volume_snapshot create error'
        snap.status = status
        snap.save()
        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=snap.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, snap, "CREATED", msg)

    def volume_snapshot_get(self, ctxt, volume_snapshot_id):
        return objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id)

    def volume_snapshot_update(self, ctxt, volume_snapshot_id, data):
        volume_snapshot = self.volume_snapshot_get(ctxt, volume_snapshot_id)
        for k, v in six.iteritems(data):
            setattr(volume_snapshot, k, v)
        volume_snapshot.save()
        return volume_snapshot

    def _snap_delete(self, ctxt, snap, extra_data):
        pool_name = extra_data['pool_name']
        volume_name = extra_data['volume_name']
        snap_name = snap.uuid
        try:
            ceph_client = CephTask(ctxt)
            # 关闭快照保护，再删除快照
            ceph_client.rbd_unprotect_snap(pool_name, volume_name, snap_name)
            ceph_client.rbd_snap_delete(pool_name, volume_name, snap_name)
            snap.destroy()
            logger.info('snapshot_delete success,snap_name={}'.format(
                snap_name))
            msg = _("delete snapshot success")
        except exception.StorException as e:
            status = s_fields.VolumeSnapshotStatus.ERROR
            snap.status = status
            snap.save()
            logger.error('snapshot_delete error,{}/{}@{},reason:{}'.format(
                pool_name, volume_name, snap_name, str(e)))
            msg = _('delete snapshot error')
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=snap.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, snap, 'DELETED', msg)

    def volume_snapshot_delete(self, ctxt, volume_snapshot_id):
        snap = objects.VolumeSnapshot.get_by_id(ctxt, volume_snapshot_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=volume_snapshot_id)
        volume = objects.Volume.get_by_id(ctxt, snap.volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=snap.volume_id)
        pool = objects.Pool.get_by_id(ctxt, volume.pool_id)
        snap_data = {'volume_name': volume.volume_name,
                     'pool_name': pool.pool_name,
                     'snap': snap}
        snap = snap_data['snap']
        snap.status = s_fields.VolumeSnapshotStatus.DELETING
        snap.save()
        self.executor.submit(self._snap_delete, ctxt, snap, snap_data)
        return snap

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

    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        ceph_conf = objects.CephConfigList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return ceph_conf

    def _get_mon_node(self, ctxt):
        mons = objects.ServiceList.get_all(
            ctxt, filters={'name': 'mon', 'cluster_id': ctxt.cluster_id})
        node = []
        for mon in mons:
            node.append(mon.node_id)
        return node

    def _get_osd_node(self, ctxt, osd_name):
        node = []
        if osd_name == "*":
            nodes = objects.NodeList.get_all(
                ctxt, filters={'cluster_id': ctxt.cluster_id,
                               'role_storage': True})
            for n in nodes:
                node.append(n.id)
        else:
            osd = objects.OsdList.get_all(
                ctxt, filters={
                    'osd_id': osd_name, 'cluster_id':
                    ctxt.cluster_id
                })[0]
            node.append(osd.id)
        return node

    def _get_all_node(self, ctxt):
        node = []
        nodes = objects.NodeList.get_all(
            ctxt, filters={'cluster_id': ctxt.cluster_id})
        for n in nodes:
            node.append(n.id)
        return node

    def _get_config_nodes(self, ctxt, values):
        group = values['group']
        key = values['key']
        value = values['value']

        nodes = []
        temp_configs = {}
        ceph_client = CephTask(ctxt)

        if group == 'global':
            if key in ClusterConfg.cluster_mon_restart_configs:
                nodes = self._get_mon_node(ctxt)
            if key in ClusterConfg.cluster_osd_restart_configs:
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in ClusterConfg.cluster_rgw_restart_configs:
                # TODO handle rgw
                pass
            if key in ClusterConfg.cluster_mon_temp_configs:
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_mon_node(ctxt)
            if key in ClusterConfg.cluster_osd_temp_configs:
                temp_configs = [{'service': 'osd.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in ClusterConfg.cluster_rgw_temp_configs:
                # TODO handle rgw
                pass

        if group.startswith('osd'):
            osd_id = group.split('.')
            if len(osd_id) == 1:
                if key in ClusterConfg.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.*',
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if len(osd_id) == 2:
                if key in ClusterConfg.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.' + osd_id[1],
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name=osd_id[1])

        if group == 'mon':
            if key in ClusterConfg.cluster_mon_temp_configs:
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
            nodes = self._get_mon_node(ctxt)

        if group == 'client':
            nodes = self._get_all_node(ctxt)

        if group == 'rgw':
            # TODO handle rgw
            pass

        if temp_configs:
            ceph_client = CephTask(ctxt)
            try:
                ceph_client.config_set(temp_configs)
            except exception.CephException as e:
                logger.error(e)
                return []
        ceph_client.ceph_config()
        return nodes

    def _ceph_confg_update(self, ctxt, nodes, values):
        for n in nodes:
            client = AgentClientManager(
                ctxt, cluster_id=ctxt.cluster_id).get_client(node_id=n)
            _success = client.ceph_config_update(ctxt, values)
            if not _success:
                logger.error(
                    'Ceph config update failed, node id: {}'.format(n)
                )
                return False
        return True

    def ceph_config_content(self, ctxt):
        content = objects.ceph_config.ceph_config_content(ctxt)
        return content

    def _ceph_config_db(self, ctxt, values):
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

    def _ceph_config_set(self, ctxt, values):
        nodes = self._get_config_nodes(ctxt, values)
        if nodes:
            _success = self._ceph_confg_update(ctxt, nodes, values)
            msg = _('Ceph config update failed')
            if _success:
                self._ceph_config_db(ctxt, values)
                msg = _('Ceph config update successful')
        else:
            msg = _('Ceph config update failed')

        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=ctxt.cluster_id).get_client()
        wb_client.send_message(ctxt, values, "UPDATED", msg)

    def ceph_config_set(self, ctxt, values):
        self.executor.submit(self._ceph_config_set(ctxt, values))
        return values

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

    ###################

    def cluster_metrics_get(self, ctxt):
        prometheus = PrometheusTool(ctxt)
        res = {}
        cluster_perf = {'cluster_read_bytes_sec', 'cluster_read_op_per_sec',
                        'cluster_recovering_bytes_per_sec',
                        'cluster_recovering_objects_per_sec',
                        'cluster_write_bytes_sec', 'cluster_write_op_per_sec',
                        'cluster_write_lat', 'cluster_read_lat',
                        'cluster_total_bytes', 'cluster_total_used_bytes'}

        for m in cluster_perf:
            metric = 'ceph_{}'.format(m)
            res[m] = prometheus.prometheus_get_metric(metric)
        prometheus.cluster_get_pg_state(res)
        return res

    def cluster_history_metrics_get(self, ctxt, start, end):
        prometheus = PrometheusTool(ctxt)
        res = {}
        cluster_perf = {'cluster_read_bytes_sec', 'cluster_read_op_per_sec',
                        'cluster_recovering_bytes_per_sec',
                        'cluster_recovering_objects_per_sec',
                        'cluster_write_bytes_sec', 'cluster_write_op_per_sec',
                        'cluster_write_lat', 'cluster_read_lat',
                        'cluster_total_bytes', 'cluster_total_used_bytes'}
        for m in cluster_perf:
            metric = 'ceph_{}'.format(m)
            res[m] = prometheus.prometheus_get_histroy_metric(
                metric, float(start), float(end))
        return res

    def node_metrics_monitor_get(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_monitor(node)
        return metrics

    def node_metrics_histroy_monitor_get(self, ctxt, node_id, start, end):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_histroy_monitor(
            node, float(start), float(end))
        return metrics

    def node_metrics_network_get(self, ctxt, node_id, net_name):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        metrics = prometheus.node_get_metrics_network(node, net_name)
        return metrics

    def node_metrics_histroy_network_get(self, ctxt, node_id, net_name, start,
                                         end):
        node = objects.Node.get_by_id(ctxt, node_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.node_get_metrics_histroy_network(
            node=node, net_name=net_name, start=float(start), end=float(end))
        return data

    def osd_metrics_get(self, ctxt, osd_id):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.osd_get_realtime_metrics(osd)
        return data

    def osd_metrics_history_get(self, ctxt, osd_id, start, end):
        osd = objects.Osd.get_by_id(ctxt, osd_id)
        prometheus = PrometheusTool(ctxt)
        data = prometheus.osd_get_histroy_metrics(osd, float(start),
                                                  float(end))
        return data

    def pool_metrics_get(self, ctxt, pool_id):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.pool_get_capacity(pool, metrics)
        prometheus.pool_get_perf(pool, metrics)
        return metrics

    def pool_metrics_history_get(self, ctxt, pool_id, start, end):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        prometheus = PrometheusTool(ctxt)
        metrics = {}
        prometheus.pool_get_histroy_capacity(pool, float(start), float(end),
                                             metrics)
        prometheus.pool_get_histroy_perf(pool, float(start), float(end),
                                         metrics)
        return metrics


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
