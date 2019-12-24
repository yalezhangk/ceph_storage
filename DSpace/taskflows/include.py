#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys

import six
import taskflow
from oslo_log import log as logging
from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow.types.failure import Failure

from DSpace import context
from DSpace import exception
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects import utils as obj_utils
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.base import BaseTask
from DSpace.taskflows.base import CompleteTask
from DSpace.taskflows.base import PrepareTask
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.crush import CrushContentGen
from DSpace.taskflows.node import DSpaceAgentInstall
from DSpace.taskflows.node import DSpaceAgentUninstall
from DSpace.taskflows.node import DSpaceChronyInstall
from DSpace.taskflows.node import DSpaceChronyUninstall
from DSpace.taskflows.node import DSpaceExpoterInstall
from DSpace.taskflows.node import DSpaceExpoterUninstall
from DSpace.taskflows.node import GetNodeInfo
from DSpace.taskflows.node import InstallDocker
from DSpace.taskflows.node import InstallDSpaceTool
from DSpace.taskflows.node import NodesCheck
from DSpace.taskflows.node import ReduceNodesInfo
from DSpace.taskflows.node import SyncCephVersion
from DSpace.taskflows.node import UninstallDSpaceTool
from DSpace.taskflows.utils import CleanDataMixin
from DSpace.tools.base import SSHExecutor
from DSpace.tools.probe import ProbeTool
from DSpace.tools.system import System as SystemTool
from DSpace.utils import cidr2network
from DSpace.utils import logical_xor

logger = logging.getLogger(__name__)

"""To include a cluser

1. create db
2. install node
3. collect info
"""


class CreateDB(BaseTask):
    default_provides = 'nodes'

    def execute(self, ctxt, datas, task_info):
        super(CreateDB, self).execute(task_info)
        # init task data
        objects.sysconfig.sys_config_set(
            ctxt, 'is_import', True
        )
        t = task_info['task']
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', t.id
        )

        # create node data
        nodes = []
        for data in datas:
            node = objects.Node(
                ctxt, ip_address=data.get('ip_address'),
                hostname=data.get('hostname'),
                password=data.get('password'),
                cluster_ip=data.get('cluster_ip'),
                public_ip=data.get('public_ip'),
                status=s_fields.NodeStatus.CREATING)
            roles = data.get('roles')
            if 'monitor' in roles:
                node.role_monitor = True
            if 'storage' in roles:
                node.role_storage = True
            node.create()
            nodes.append(node)
        return nodes


class SyncNodeInfo(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(SyncNodeInfo, self).execute(task_info)
        sys_tool = SystemTool(node.executer)
        node_infos = sys_tool.get_node_baseinfo()
        node.hostname = node_infos.get('hostname')
        node.save()


class MarkNodeActive(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(MarkNodeActive, self).execute(task_info)
        node.status = s_fields.NodeStatus.ACTIVE
        node.save()


class InstallService(BaseTask):
    def execute(self, ctxt, nodes, task_info):
        super(InstallService, self).execute(task_info)
        all_node_install_wf = uf.Flow('Nodes Install')
        kwargs = {}
        sync_version = True
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            node_install_flow = lf.Flow('Node Install %s' % node.id)
            node_install_flow.add(SyncNodeInfo(
                "Sync Node Info %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(InstallDocker(
                "Intall Docker %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceChronyInstall(
                "DSpace Chrony Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceExpoterInstall(
                "DSpace Exporter Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(InstallDSpaceTool(
                "DSpace Tool Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceAgentInstall(
                "DSpace Agent Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(MarkNodeActive(
                "Mark node active %s" % node.id,
                rebind={'node': arg}))
            if sync_version:
                node_install_flow.add(SyncCephVersion(
                    "Sync Ceph Version %s" % node.id,
                    rebind={'node': arg}))
                sync_version = False
            all_node_install_wf.add(node_install_flow)
            kwargs[arg] = node
        kwargs.update({
            "ctxt": ctxt,
            'task_info': {}
        })
        engines.run(all_node_install_wf, store=kwargs,
                    engine='parallel')
        logger.info("Install Service flow run success")
        return True

    def revert(self, nodes, result, flow_failures):
        if isinstance(result, Failure):
            for node in nodes:
                node.status = s_fields.NodeStatus.ERROR
                node.save()

    def get_ssh_executor(self, node):
        return SSHExecutor(hostname=str(node.ip_address),
                           password=node.password)


class UninstallService(BaseTask):
    def execute(self, ctxt, task_info):
        super(UninstallService, self).execute(task_info)
        wf = lf.Flow('TaskFlow')
        kwargs = {}
        nodes = objects.NodeList.get_all(ctxt)
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            wf.add(DSpaceChronyUninstall(
                "DSpace Chrony Unintall %s" % node.id,
                rebind={'node': arg}))
            wf.add(DSpaceExpoterUninstall(
                "DSpace Exporter Unintall %s" % node.id,
                rebind={'node': arg}))
            wf.add(UninstallDSpaceTool(
                "DSpace Tool UnIntall %s" % node.id,
                rebind={'node': arg}))
            wf.add(DSpaceAgentUninstall(
                "DSpace Agent Unintall %s" % node.id,
                rebind={'node': arg}))
            kwargs[arg] = node
        kwargs.update({
            "ctxt": ctxt,
            'task_info': {}
        })
        taskflow.engines.run(wf, store=kwargs)
        logger.info("Install Service flow run success")

    def revert(self, task_info, result, flow_failures):
        pass

    def get_ssh_executor(self, node):
        return SSHExecutor(hostname=str(node.ip_address),
                           password=node.password)


class SyncCephConfig(BaseTask):
    def execute(self, ctxt, nodes, task_info):
        super(SyncCephConfig, self).execute(task_info)
        for node in nodes:
            tool = ProbeTool(node.executer)
            ceph_configs = tool.probe_ceph_config()
            logger.info(ceph_configs)
            for section in ceph_configs:
                for key, value in six.iteritems(ceph_configs.get(section)):
                    key = key.replace(" ", "_")
                    self._update_config(ctxt, section, key, value)
            admin_keyring = tool.probe_admin_keyring()
            if admin_keyring:
                logger.info(admin_keyring)
                key = admin_keyring.get('entity')
                value = admin_keyring.get('key')
                self._update_config(ctxt, "keyring", key, value)

    def _update_config(self, ctxt, section, key, value):
        objs = objects.CephConfigList.get_all(
            ctxt, filters={"group": section, "key": key}
        )
        if objs:
            obj = objs[0]
            if value != obj.value:
                logger.warning("config diff on node: [%s]%s=%s diff db %s",
                               section, key, value, obj.value)
        else:
            ceph_conf = objects.CephConfig(
                ctxt, group=section, key=key,
                value=value,
                value_type="string",
                display_description=None,
                cluster_id=ctxt.cluster_id)
            ceph_conf.create()


class SyncClusterInfo(BaseTask):
    def execute(self, ctxt, nodes, task_info):
        """Sync Cluster info

        1. set node role(role set in node craate).
        2. update osd info
        3. update pool info
        """
        super(SyncClusterInfo, self).execute(task_info)
        for node in nodes:
            tool = ProbeTool(node.executer)
            osd_infos = tool.probe_node_osd()
            logger.info(osd_infos)
            for info in osd_infos:
                self._update_osd(ctxt, info, node)
        self._probe_pool(ctxt)
        self._update_disk_status(ctxt)
        self._update_planning(ctxt)

    def _update_disk_status(self, ctxt):
        disks = objects.DiskList.get_all(
            ctxt, expected_attrs=['partition_used'])
        for disk in disks:
            if disk.partition_used < disk.partition_num:
                disk.status = s_fields.DiskStatus.AVAILABLE
            else:
                disk.status = s_fields.DiskStatus.INUSE
            disk.save()

    def _get_or_create_rack(self, ctxt, rack_id):
        rack_objs = objects.RackList.get_all(ctxt, filters={"id": rack_id})
        if not rack_objs:
            rack_obj = objects.Rack(ctxt, id=rack_id, name="rack-%" % rack_id)
            rack_obj.create()
        else:
            rack_obj = rack_objs[0]
        return rack_obj

    def _get_or_create_dc(self, ctxt, dc_id):
        dc_objs = objects.DatacenterList.get_all(ctxt, filters={"id": dc_id})
        if not dc_objs:
            dc_obj = objects.Rack(ctxt, id=dc_id, name="datacenter-%s" % dc_id)
            dc_obj.create()
        else:
            dc_obj = dc_objs[0]
        return dc_obj

    def _update_host_rack(self, ctxt, crush_host, rack_id):
        res = crush_host.split("-")
        if not res:
            raise exception.Invalid(_("Crush hostname Invalid"))
        hostname = res[-1]
        hosts = objects.Node.get_all(ctxt, filters={"hostname": hostname})
        if hosts:
            host = hosts[0]
            host.rack_id = rack_id
            host.save()
        raise exception.NodeNotFound(node_id=hostname)

    def _update_rack_dc(self, ctxt, crush_rack, dc_id):
        res = crush_rack.split("-")
        if not res:
            raise exception.Invalid(_("Crush hostname Invalid"))
        rack_id = res[-1][4:]
        rack = objects.Rack.get_by_id(ctxt, rack_id)
        rack.datacenter_id = dc_id
        rack.save()

    def _get_osds_from_crush(self, ctxt, ceph_client, fault_domain,
                             root_buckets):
        osds = []
        if fault_domain == "host":
            hosts = root_buckets
            for host in hosts:
                host_osds = ceph_client.get_bucket_info(host)
                osds.extend(host_osds)
        elif fault_domain == "rack":
            racks = root_buckets
            for rack_name in racks:
                logger.info("---rack info: %s", rack_name)
                res = re.search(r'rack(\d)', rack_name)
                if not res:
                    raise exception.Invalid(_("Rack Invalid"))
                rack_id = res.group()
                self._get_or_create_rack(ctxt, rack_id)
                hosts = ceph_client.get_bucket_info(rack_name)
                for host in hosts:
                    host_osds = ceph_client.get_bucket_info(host)
                    osds.extend(host_osds)
                    self._update_host_rack(ctxt, host, rack_id)

        elif fault_domain == "datacenter":
            datacenters = root_buckets
            for datacenter_name in datacenters:
                logger.info("---dc info: %s", datacenter_name)
                res = re.search(r'datacenter(\d)', datacenter_name)
                if not res:
                    raise exception.Invalid(_("Datacenter Invalid"))
                datacenter_id = res.group()
                self._get_or_create_dc(ctxt, datacenter_id)
                racks = ceph_client.get_bucket_info(datacenter_name)
                for rack_name in racks:
                    logger.info("---rack info: %s", rack_name)
                    res = re.search(r'rack(\d)', rack_name)
                    if not res:
                        raise exception.Invalid(_("Rack Invalid"))
                    rack_id = res.group()
                    self._get_or_create_rack(ctxt, rack_id)
                    hosts = ceph_client.get_bucket_info(rack_name)
                    self._update_rack_dc(ctxt, rack_name, datacenter_id)
                    for host in hosts:
                        host_osds = ceph_client.get_bucket_info(host)
                        osds.extend(host_osds)
                        self._update_host_rack(ctxt, host, rack_id)
        return osds

    def _update_planning(self, ctxt):
        logger.info("update planning")
        nodes = objects.NodeList.get_all(ctxt, filters={"rack_id": None})
        logger.info("nodes %s need rack_id", nodes)
        if nodes:
            rack = obj_utils.rack_create(ctxt)
            logger.info("create new rack %s" % rack.name)
            for node in nodes:
                node.rack_id = rack.id
                node.save()
        racks = objects.RackList.get_all(ctxt, filters={"datacenter_id": None})
        logger.info("racs %s need datacenter_id", racks)
        if racks:
            dc = obj_utils.datacenter_create(ctxt)
            logger.info("create new datacenter %s" % dc.name)
            for rack in racks:
                rack.datacenter_id = dc.id
                rack.save()

    def _get_host_by_osd_name(self, ctxt, osd_name):
        osd = objects.Osd.get_by_osd_name(
            ctxt, osd_name, expected_attrs=['node'])
        return osd.node

    def _get_rack_by_host(self, ctxt, host):
        rack = objects.Rack.get_by_id(ctxt, host.rack_id,
                                      expected_attrs=['node'])
        return rack

    def _rack_create(self, ctxt, rack_name):
        logger.info("create rack: %s", rack_name)
        rack = objects.Rack(ctxt, name=rack_name)
        rack.create()
        return rack

    def _nodes_update_rack(self, ctxt, nodes, rack_id):
        for node in nodes:
            node.rack_id = rack_id
            node.save()

    def _datacenter_create(self, ctxt, dc_name):
        logger.info("create datacenter: %s", dc_name)
        dc = objects.Datacenter(ctxt, name=dc_name)
        dc.create()
        return dc

    def _racks_update_datacenter(self, ctxt, racks, datacenter_id):
        for rack in racks:
            rack.datacenter_id = datacenter_id
            rack.save()

    def _add_plan_by_crush(self, ctxt, crush_info):
        host_map = {}
        rack_map = {}
        crush_hosts = crush_info['hosts']
        for host_crush_name, osd_names in six.iteritems(crush_hosts):
            if not osd_names:
                raise exception.ProgrammingError(
                    reason="empty host(%s) not allowed" % host_crush_name)
            host = self._get_host_by_osd_name(ctxt, osd_names[0])
            host_map[host_crush_name] = host

        crush_racks = crush_info['racks']
        for rack_crush_name, crush_hosts in six.iteritems(crush_racks):
            if not crush_hosts:
                raise exception.ProgrammingError(
                    reason="empty rack(%s) not allowed" % rack_crush_name)
            host_crush_name = crush_hosts[0]
            if host_crush_name not in host_map:
                raise exception.ProgrammingError(
                    reason="host(%s) not found" % host_crush_name)
            host = host_map[host_crush_name]
            if not host.rack_id:
                rack = self._rack_create(ctxt, rack_crush_name)
                hosts = [host_map[name] for name in crush_hosts]
                self._nodes_update_rack(ctxt, hosts, rack.id)
            if host.rack_id:
                rack = objects.Rack.get_by_id(ctxt, host.rack_id)
            rack_map[rack_crush_name] = rack

        crush_dcs = crush_info['datacenters']
        for dc_crush_name, crush_racks in six.iteritems(crush_dcs):
            if not crush_racks:
                raise exception.ProgrammingError(
                    reason="empty datacenter(%s) not allowed" % dc_crush_name)
            rack_crush_name = crush_racks[0]
            if rack_crush_name not in rack_map:
                raise exception.ProgrammingError(
                    reason="rack(%s) not found" % rack_crush_name)
            rack = rack_map[rack_crush_name]
            if not rack.datacenter_id:
                dc = self._datacenter_create(ctxt, dc_crush_name)
                racks = [rack_map[name] for name in crush_racks]
                self._racks_update_datacenter(ctxt, racks, dc.id)

    def _crush_db_create(self, ctxt, crush_info):
        rule_name = crush_info.get("rule_name")
        root_name = crush_info.get("root_name")
        fault_domain = crush_info.get("type")
        osd_ids = crush_info.get("osds").keys()
        osd_ids = [o.replace("osd.", "") for o in osd_ids]
        osds = objects.OsdList.get_all(ctxt, filters={"osd_id": osd_ids})
        gen = CrushContentGen(
            ctxt, rule_name=rule_name, root_name=root_name,
            fault_domain=fault_domain, osds=osds
        )
        gen.gen_content()
        content = gen.map_exists(crush_info)
        crush = objects.CrushRule(
            ctxt,
            rule_name=rule_name,
            type=crush_info.get("type"),
            rule_id=crush_info.get("rule_id"),
            content=content
        )
        crush.create()
        for osd in osds:
            osd.crush_rule_id = crush.id
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
        return crush

    def _get_crush_info(self, ctxt, ceph_client, rule_name):
        crushs = objects.CrushRuleList.get_all(
            ctxt, filters={"rule_name": rule_name})
        if crushs:
            crush = crushs[0]
        else:
            rule_info = ceph_client.get_crush_rule_info(rule_name)
            self._add_plan_by_crush(ctxt, rule_info)
            crush = self._crush_db_create(ctxt, rule_info)
        return crush

    def _create_pool(self, ctxt, pool_info):
        pool = objects.Pool(
            ctxt, crush_rule_id=pool_info['crush_rule_id'],
            status=s_fields.PoolStatus.ACTIVE,
            pool_name=pool_info.get("pool_name"),
            display_name=pool_info.get("pool_name"),
            type="replicated",
            role="data",
            data_chunk_num=None,
            coding_chunk_num=None,
            osd_num=pool_info['osd_num'],
            speed_type="hdd",
            replicate_size=pool_info.get("rep_size"),
            failure_domain_type=pool_info.get("fault_domain")
        )
        pool.create()

    def _probe_pool(self, ctxt):
        ceph_client = CephTask(ctxt)
        all_pools = ceph_client.get_pools()
        logger.info("get all pools: %s", all_pools)
        for pool in all_pools:
            # TODO: EC pool get info
            pool_id = pool.get("poolnum")
            pool_name = pool.get("poolname")
            pool_info = ceph_client.get_pool_info(pool_name, "crush_rule")
            logger.info("rados get pool info: %s", pool_info)

            # {'pool': 'rbd', 'pool_id': 2, 'size': 3}
            pool_size = ceph_client.get_pool_info(pool_name, "size")
            rep_size = pool_size.get("size")
            rule_name = pool_info.get("crush_rule")
            crush = self._get_crush_info(ctxt, ceph_client, rule_name)
            pool = objects.Pool(
                ctxt, crush_rule_id=crush.id,
                pool_id=pool_id,
                status=s_fields.PoolStatus.ACTIVE,
                pool_name=pool_name,
                display_name=pool_name,
                type="replicated",
                role="data",
                data_chunk_num=None,
                coding_chunk_num=None,
                osd_num=len(crush.content['osds']),
                speed_type="hdd",
                replicate_size=rep_size,
                failure_domain_type=crush.type
            )
            pool.create()

    def _update_osd(self, ctxt, osd_info, node):
        logger.info("sync node_id %s, osd %s", node.id, osd_info)
        diskname = osd_info.get('disk')
        disk = self._get_disk(ctxt, diskname, node.id)
        disk.status = s_fields.DiskStatus.INUSE
        disk.role = s_fields.DiskRole.DATA
        disk.save()

        osd = objects.Osd(
            ctxt, node_id=node.id,
            fsid=osd_info.get('fsid'),
            osd_id=osd_info.get('osd_id'),
            type=osd_info.get('type', s_fields.OsdType.BLUESTORE),
            disk_type=disk.type,
            status=s_fields.OsdStatus.ACTIVE,
            disk_id=disk.id
        )
        osd.create()

        if "block.db" in osd_info:
            part_name = osd_info["block.db"]
            part = self._get_part(ctxt, part_name, node.id)
            part.role = s_fields.DiskPartitionRole.DB
            part.status = s_fields.DiskStatus.INUSE
            part.save()
            osd.db_partition_id = part.id
            disk = self._get_disk_by_part(ctxt, part_name, node.id)
            disk.role = s_fields.DiskRole.ACCELERATE
            disk.save()
        if "block.wal" in osd_info:
            part_name = osd_info["block.wal"]
            part = self._get_part(ctxt, part_name, node.id)
            part.role = s_fields.DiskPartitionRole.WAL
            part.status = s_fields.DiskStatus.INUSE
            part.save()
            osd.wal_partition_id = part.id
            disk = self._get_disk_by_part(ctxt, part_name, node.id)
            disk.role = s_fields.DiskRole.ACCELERATE
            disk.save()
        if "block.t2ce" in osd_info:
            part_name = osd_info["block.t2ce"]
            part = self._get_part(ctxt, part_name, node.id)
            part.role = s_fields.DiskPartitionRole.CACHE
            part.status = s_fields.DiskStatus.INUSE
            part.save()
            osd.cache_partition_id = part.id
            disk = self._get_disk_by_part(ctxt, part_name, node.id)
            disk.role = s_fields.DiskRole.ACCELERATE
            disk.save()
        if "journal" in osd_info:
            part_name = osd_info["journal"]
            part = self._get_part(ctxt, part_name, node.id)
            part.role = s_fields.DiskPartitionRole.JOURNAL
            part.status = s_fields.DiskStatus.INUSE
            part.save()
            osd.journal_partition_id = part.id
            disk = self._get_disk_by_part(ctxt, part_name, node.id)
            disk.role = s_fields.DiskRole.ACCELERATE
            disk.save()
        osd.save()

    def _mark_part_used(self, ctxt, disk_id):
        parts = objects.DiskPartitionList.get_all(ctxt, filters={
            "disk_id": disk_id
        })
        if not parts:
            return
        for part in parts:
            part.status = s_fields.DiskStatus.INUSE
            part.role = s_fields.DiskPartitionRole.DATA
            part.save()

    def _get_disk(self, ctxt, diskname, node_id):
        disks = objects.DiskList.get_all(ctxt, filters={
            'name': diskname, "node_id": node_id
        })
        if disks:
            return disks[0]
        else:
            raise exception.DiskNotFound(disk_id=diskname)

    def _get_disk_by_part(self, ctxt, part_name, node_id):
        while part_name[-1].isdigit():
            part_name = part_name[:-1]
        return self._get_disk(ctxt, part_name, node_id)

    def _get_part(self, ctxt, part_name, node_id):
        parts = objects.DiskPartitionList.get_all(ctxt, filters={
            'name': part_name, "node_id": node_id
        })
        if parts:
            return parts[0]
        else:
            raise exception.DiskPartitionNotFound(disk_part_id=part_name)


class UpdateIncludeFinish(BaseTask):
    def execute(self, ctxt, task_info):
        """Mark Include Finish"""
        super(UpdateIncludeFinish, self).execute(task_info)
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', -1
        )

    def revert(self, task_info, result, flow_failures):
        pass


class PrepareClean(BaseTask):
    def execute(self, ctxt, task_info):
        super(PrepareClean, self).execute(task_info)
        # init task data
        t = task_info['task']
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', t.id
        )


class CleanIncludeCluster(BaseTask, CleanDataMixin):
    def execute(self, ctxt, task_info):
        """Clean All Include Data

        1. clean pool
        2. clean osd
        3. clean crush rule
        4. clean disk partition
        5. clean disk
        6. clean network
        6. clean node
        """
        super(CleanIncludeCluster, self).execute(task_info)

        self._clean_pool(ctxt)
        self._clean_osd(ctxt)
        self._clean_crush_rule(ctxt)
        self._clean_disk_partition(ctxt)
        self._clean_disk(ctxt)
        self._clean_network(ctxt)
        self._clean_node(ctxt)
        self._clean_ceph_config(ctxt)
        self._clean_rack(ctxt)
        self._clean_datacenter(ctxt)

        # mark finish
        objects.sysconfig.sys_config_set(
            ctxt, 'is_import', False
        )
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', -1
        )


class GetClusterCheckInfo(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(GetClusterCheckInfo, self).execute(task_info)
        tool = ProbeTool(node.executer)
        try:
            data = tool.cluster_check()
        except exception.SSHAuthInvalid:
            # exception info will auto add to end
            logger.warning("%s connect error")
            data = {}
        return data


class InclusionNodesCheck(NodesCheck):
    def __init__(self, *args, **kwargs):
        super(InclusionNodesCheck, self).__init__(*args, **kwargs)

    def _node_inclusion_check_version(self, data):
        pkgs = data.get('ceph_version')
        installed = pkgs.get('installed')
        if not installed:
            return {
                "check": False,
                "msg": _("Ceph package not found")
            }
        elif not self._check_compatibility(installed):
            return {
                "check": False,
                "msg": _("Installed version not support")
            }
        return {
                "check": True,
                "msg": None
        }

    def _node_check_roles(self, roles, services):
        logger.info("check roles: roles(%s), services(%s)", roles, services)
        res = {}
        # add default value
        roles = roles or []
        for s in roles:
            res[s] = True

        # update value
        if logical_xor("ceph-osd" in services, "storage" in roles):
            res['storage'] = False
        if logical_xor("ceph-mon" in services, "monitor" in roles):
            res['monitor'] = False

        data = []
        for k, v in six.iteritems(res):
            data.append({
                'role': k,
                'status': v
            })
        return data

    def _inclusion_check_admin_ips(self,  datas):
        """Check admin_ips in nodes

        :return list: The admin_ip not in datas
        """
        cluster = objects.Cluster.get_by_id(self.ctxt, self.ctxt.cluster_id)
        if not cluster.is_admin:
            return []
        admin_ips = objects.sysconfig.sys_config_get(
            self.ctxt, ConfigKey.ADMIN_IPS)
        if admin_ips:
            admin_ips = admin_ips.split(',')
        for data in datas:
            admin_ip = data.get("admin_ip")
            if admin_ip in admin_ips:
                admin_ips.pop(admin_ip)
        return admin_ips

    def _inclusion_check_public_ip(self, datas, infos):
        res = {}
        submit_ips = [item.get('public_ip') for item in datas]
        leaks = []
        for n in infos:
            if n not in submit_ips:
                leaks.append(n)
        extras = []
        for n in submit_ips:
            if n not in infos:
                extras.append(n)
        res['leak_public_ips'] = leaks
        res['extra_public_ips'] = extras
        logger.info("leak_public_ips: %s", res['leak_public_ips'])
        logger.info("extra_public_ips: %s", res['extra_public_ips'])
        return res

    def _inclusion_check_network(self, infos):
        logger.info("inclusion check network infos: %s", infos)
        res = {}
        public_cidr = objects.sysconfig.sys_config_get(
            self.ctxt, key="public_cidr"
        )
        ceph_public_cidr = infos.get('public_network')
        logger.info("cluster public_cidr(%s), ceph public_network(%s)",
                    public_cidr, ceph_public_cidr)

        if not ceph_public_cidr:
            res["public_network"] = {
                "check": False,
                "msg": _("Ceph public_network not found")
            }
        elif cidr2network(public_cidr) == cidr2network(ceph_public_cidr):
            res["public_network"] = {
                "check": True,
                "msg": ""
            }
        else:
            res["public_network"] = {
                "check": False,
                "msg": _("Ceph public_network not equal to cluster "
                         "public_network")
            }
        cluster_cidr = objects.sysconfig.sys_config_get(
            self.ctxt, key="cluster_cidr"
        )
        ceph_cluster_cidr = infos.get('cluster_network')
        logger.info("cluster cluster_cidr(%s), ceph cluster_network(%s)",
                    cluster_cidr, ceph_cluster_cidr)

        if not ceph_cluster_cidr:
            res["cluster_network"] = {
                "check": False,
                "msg": _("Ceph cluster_network not found")
            }
        elif cidr2network(cluster_cidr) == cidr2network(ceph_cluster_cidr):
            res["cluster_network"] = {
                "check": True,
                "msg": ""
            }
        else:
            res["cluster_network"] = {
                "check": False,
                "msg": _("Ceph cluster_network not equal to cluster "
                         "cluster_network")
            }
        return res

    def check(self, datas):
        datas_map = {}
        nodes_map = {}
        logger.info("check inclusion data: %s", datas)
        checks = {}
        node_checks = {}
        checks['leak_admin_ips'] = self._inclusion_check_admin_ips(datas)
        logger.info("leak_admin_ips: %s", checks['leak_admin_ips'])
        for item in datas:
            item["roles"] = item["roles"].split(',')
            admin_ip = item.get('ip_address')
            datas_map[admin_ip] = item
            node_checks[admin_ip], skip = self._check_ip_by_db(item)
            node_checks[admin_ip]['admin_ip'] = admin_ip
            if not skip:
                node = self._get_node(item)
                nodes_map[admin_ip] = node
                node_checks[admin_ip]['check'] = {
                    "check": True,
                    "msg": ""
                }
            else:
                node_checks[admin_ip]['check'] = {
                    "check": False,
                    "msg": _("Admin ip(%s) alreay in platform") % admin_ip
                }

        nodes = list(six.itervalues(nodes_map))
        if not nodes:
            checks['nodes'] = [v for v in six.itervalues(node_checks)]
            return checks

        infos = self.get_infos(nodes)

        cluster_check = infos.pop('cluster_check')
        checks.update(
            self._inclusion_check_public_ip(datas, cluster_check['nodes']))
        checks['check_planning'] = cluster_check['check_crush']
        checks.update(
            self._inclusion_check_network(cluster_check['configs']))

        for info in six.itervalues(infos):
            admin_ip = str(info.get('node').ip_address)
            item = datas_map[admin_ip]
            res = {}
            if "hostname" not in info:
                res['check_through'] = False
                continue
            res.update(self._common_check(info))
            res['ceph_version'] = self._node_inclusion_check_version(info)
            res['check_Installation_package'] = info.get("ceph_package")
            res['check_roles'] = self._node_check_roles(
                item['roles'], info.get("ceph_service"))
            res['check_ceph_port'] = self._check_ceph_port(info.get("ports"))
            res['check_version'] = self._node_inclusion_check_version(info)
            res.update(self._check_network(info['node'], info['network']))
            node_checks[admin_ip].update(res)
        checks['nodes'] = [v for v in six.itervalues(node_checks)]
        return checks

    def get_infos(self, nodes):
        logger.info("Get nodes info")
        store = {}
        provided = []
        wf = lf.Flow('NodesCheckTaskFlow')
        nodes_wf = uf.Flow("GetNodesInof")
        for node in nodes:
            ip = str(node.ip_address)
            arg = "node-%s" % ip
            provided.append("info-%s" % ip)
            wf.add(GetNodeInfo("GetNodeInfo-%s" % ip,
                               provides=provided[-1],
                               rebind={'node': arg}))
            store[arg] = node
        wf.add(GetClusterCheckInfo("cluster_check"))
        store["node"] = nodes[0]
        store.update({
            "ctxt": self.ctxt,
            "info_names": ["ceph_version", 'hostname', 'ceph_package',
                           "firewall", "containers", "ports", "selinux",
                           "network", 'ceph_service'],
            "prefix": "info-",
            'task_info': {}
        })
        wf.add(nodes_wf)
        wf.add(ReduceNodesInfo("reducer", requires=provided))
        e = engines.load(wf, engine='parallel', store=store,
                         max_workers=CONF.taskflow_max_workers)
        e.run()
        infos = e.storage.get('reducer')
        infos['cluster_check'] = e.storage.get('cluster_check')
        return infos


def include_flow(ctxt, t, datas):
    # update task
    t.step_num = 5
    t.save()

    # crate task flow
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("Task prepare"))
    wf.add(CreateDB("Database add info"))
    wf.add(InstallService("Install Services"))
    wf.add(SyncCephConfig("Sync Ceph Config"))
    wf.add(SyncClusterInfo("Sync Cluster Info"))
    wf.add(UpdateIncludeFinish("Update Include Finish"))
    wf.add(CompleteTask('Complete'))
    engines.run(wf, store={
        "ctxt": ctxt,
        "datas": datas,
        'task_info': {
            "task": t,
        }
    })
    logger.info("Include flow run success")


def include_clean_flow(ctxt, t):
    # update task
    t.step_num = 4
    t.save()

    # crate task flow
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("Task prepare"))
    wf.add(PrepareClean("Prepare Clean"))
    wf.add(UninstallService("Uninstall Services"))
    wf.add(CleanIncludeCluster("Clean Include Cluster"))
    wf.add(CompleteTask('Complete'))
    engines.run(wf, store={
        "ctxt": ctxt,
        'task_info': {
            "task": t,
        }
    })
    logger.info("Include Clean flow run success")


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = context.get_context()
    ctxt.cluster_id = '358fc820-ae9c-44e3-8090-790882161b1e'
    t = objects.Task(
        ctxt,
        name="Example",
        description="Example",
        current="",
        step_num=0,
        status=s_fields.TaskStatus.RUNNING,
        step=0
    )
    t.create()
    datas = [{
        "ip_address": "192.168.103.140",
        "cluster_ip": "192.168.103.140",
        "public_ip": "192.168.103.140"
    }]
    include_flow(ctxt, t, datas)


if __name__ == '__main__':
    main()
