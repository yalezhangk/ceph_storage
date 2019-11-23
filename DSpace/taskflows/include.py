#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

import six
import taskflow.engines
from oslo_log import log as logging
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow.types.failure import Failure

from DSpace import context
from DSpace import exception
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.objects import fields as s_fields
from DSpace.taskflows.base import BaseTask
from DSpace.taskflows.base import CompleteTask
from DSpace.taskflows.base import PrepareTask
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import DSpaceAgentInstall
from DSpace.taskflows.node import DSpaceAgentUninstall
from DSpace.taskflows.node import DSpaceChronyInstall
from DSpace.taskflows.node import DSpaceChronyUninstall
from DSpace.taskflows.node import DSpaceExpoterInstall
from DSpace.taskflows.node import DSpaceExpoterUninstall
from DSpace.taskflows.node import InstallDocker
from DSpace.taskflows.utils import CleanDataMixin
from DSpace.tools.base import SSHExecutor
from DSpace.tools.probe import ProbeTool

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
            node.create()
            nodes.append(node)
        return nodes


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
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            node_install_flow = lf.Flow('Node Install %s' % node.id)
            node_install_flow.add(InstallDocker(
                "Intall Docker %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceChronyInstall(
                "DSpace Chrony Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceExpoterInstall(
                "DSpace Exporter Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(DSpaceAgentInstall(
                "DSpace Agent Intall %s" % node.id,
                rebind={'node': arg}))
            node_install_flow.add(MarkNodeActive(
                "Mark node active  %s" % node.id,
                rebind={'node': arg}))
            all_node_install_wf.add(node_install_flow)
            kwargs[arg] = node
        kwargs.update({
            "ctxt": ctxt,
            'task_info': {}
        })
        taskflow.engines.run(all_node_install_wf, store=kwargs,
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

    def _update_config(self, ctxt, section, key, value):
        objs = objects.CephConfigList.get_all(
            ctxt, filters={"section": section, "key": key}
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
        self.pool_probe(ctxt)

    def revert(self, task_info, result, flow_failures):
        pass

    def _get_osds_from_crush(self, ceph_client, fault_domain, root_buckets):
        osds = []
        if fault_domain == "host":
            hosts = root_buckets
            for host in hosts:
                host_osds = ceph_client.get_bucket_info(host)
                osds.extend(host_osds)
        elif fault_domain == "rack":
            racks = root_buckets
            for rack in racks:
                hosts = ceph_client.get_bucket_info(rack)
                for host in hosts:
                    host_osds = ceph_client.get_bucket_info(host)
                    osds.extend(host_osds)
        elif fault_domain == "datacenter":
            datacenters = root_buckets
            for datacenter in datacenters:
                racks = ceph_client.get_bucket_info(datacenter)
                for rack in racks:
                    hosts = ceph_client.get_bucket_info(rack)
                    for host in hosts:
                        host_osds = ceph_client.get_bucket_info(host)
                        osds.extend(host_osds)
        return osds

    def _get_crush_info(self, ctxt, ceph_client, rule_name):
        rule_info = ceph_client.get_crush_rule_info(rule_name)
        root_name = None
        fault_domain = None
        if "steps" in rule_info:
            for op_info in rule_info.get("steps"):
                if op_info.get("op") == "take":
                    root_name = op_info.get("item_name")
                elif op_info.get("op") == "chooseleaf_firstn":
                    fault_domain = op_info.get("type")
        root_buckets = ceph_client.get_bucket_info(root_name)
        osds = self._get_osds_from_crush(ceph_client, fault_domain,
                                         root_buckets)
        crush_rule_info = {
            "rule_name": rule_name,
            "rule_id": rule_info.get("rule_id"),
            "type": fault_domain,
            "osds": osds,
            "osd_num": len(osds)
        }
        return crush_rule_info

    def _probe_pool_info(self, ctxt):
        logger.info("trying to probe cluster pool info")
        try:
            ceph_client = CephTask(ctxt)
            all_pools = ceph_client.get_pools()
            logger.info("get all pools: %s", all_pools)
            pool_infos = []
            crush_rule_infos = {}
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
                if rule_name not in crush_rule_infos:
                    rule_info = self._get_crush_info(
                        ctxt, ceph_client, rule_name)
                    crush_rule_infos[rule_name] = rule_info
                else:
                    rule_info = crush_rule_infos[rule_name]
                logger.info("rados get rule info: %s", rule_info)
                pool_info = {
                    "pool_name": pool_name,
                    "pool_id": pool_id,
                    "crush_rule": rule_name,
                    "fault_domain": rule_info['type'],
                    "rep_size": rep_size,
                    "osd_num": rule_info['osd_num']
                }
                pool_infos.append(pool_info)
        except exception.StorException as e:
            logger.error("pool probe error: {}".format(e))
            return None
        return pool_infos, crush_rule_infos

    def _update_osd_crush_info(self, ctxt, crush_rule, crush_rule_info):
        osds = crush_rule_info['osds']
        for osd_id in osds:
            osd = objects.Osd.get_by_osd_id(ctxt, osd_id.replace("osd.", ""))
            osd.crush_rule_id = crush_rule.id
            osd.save()

    def pool_probe(self, ctxt):
        pool_infos, crush_rule_infos = self._probe_pool_info(ctxt)
        logger.info("probe pools info: %s", pool_infos)
        logger.info("probe crush info: %s", crush_rule_infos)
        for _ign, crush_rule_info in six.iteritems(crush_rule_infos):
            crush_rule = objects.CrushRule(
                ctxt, cluster_id=ctxt.cluster_id,
                rule_name=crush_rule_info.get("rule_name"),
                type=crush_rule_info.get("type"),
                content=None)
            crush_rule.create()
            self._update_osd_crush_info(ctxt, crush_rule, crush_rule_info)
        for pool_info in pool_infos:
            rule_name = pool_info.get("crush_rule")
            crush_rule = objects.CrushRuleList.get_all(
                ctxt, filters={"rule_name": rule_name})
            crush_rule_id = crush_rule[0].id
            # TODO: EC pool get info
            # TODO: update pool meta in web
            pool = objects.Pool(
                ctxt, crush_rule_id=crush_rule_id,
                cluster_id=ctxt.cluster_id,
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

        # mark finish
        objects.sysconfig.sys_config_set(
            ctxt, 'is_import', False
        )
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', -1
        )


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
    taskflow.engines.run(wf, store={
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
    taskflow.engines.run(wf, store={
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
