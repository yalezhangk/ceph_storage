#!/usr/bin/env python
# -*- coding: utf-8 -*-

import taskflow.engines
from oslo_log import log as logging
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from DSpace import objects
from DSpace.taskflows.base import BaseTask
from DSpace.taskflows.base import CompleteTask
from DSpace.taskflows.base import PrepareTask
from DSpace.taskflows.node import CephPackageUninstall
from DSpace.taskflows.node import DSpaceAgentUninstall
from DSpace.taskflows.node import DSpaceChronyUninstall
from DSpace.taskflows.node import DSpaceExpoterUninstall
from DSpace.taskflows.node import MonUninstall
from DSpace.taskflows.node import OsdUninstall
from DSpace.taskflows.node import StorageUninstall
from DSpace.taskflows.utils import CleanDataMixin
from DSpace.tools.base import SSHExecutor

logger = logging.getLogger(__name__)

"""To include a cluser

1. create db
2. install node
3. collect info
"""


class UninstallCeph(BaseTask):
    def execute(self, ctxt, task_info):
        super(UninstallCeph, self).execute(task_info)
        logger.info("Uninstall ceph cluster flow start")
        wf = lf.Flow('Clean ceph')
        kwargs = {}
        # clear iscsi target
        bgw_nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                "role_block_gateway": True
            }
        )
        for node in bgw_nodes:
            # TODO clear iscsi
            pass
        # clear ceph osd
        osds = objects.OsdList.get_all(ctxt, expected_attrs=['node', 'disk'])
        clear_osd_flow = uf.Flow('Clean osd')
        for osd in osds:
            osd.node.executer = self.get_ssh_executor(osd.node)
            arg = "osd-%s" % osd.id
            clear_osd_flow.add(OsdUninstall(
                "Uninstall %s osd" % osd.id,
                rebind={'osd': arg}))
            kwargs[arg] = osd
        wf.add(clear_osd_flow)

        # clear storage role package
        osd_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_storage": True})
        clear_storage_flow = uf.Flow('Clean storage package')
        for node in osd_nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            clear_storage_flow.add(StorageUninstall(
                "Uninstall %s storage" % node.id,
                rebind={'node': arg}))
            kwargs[arg] = node
        wf.add(clear_storage_flow)

        # clear ceph mon
        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True})
        clear_mon_flow = uf.Flow('Clean mon')
        for node in mon_nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            clear_mon_flow.add(MonUninstall(
                "Uninstall %s monitor" % node.id,
                rebind={'node': arg}))
            kwargs[arg] = node
        wf.add(clear_mon_flow)

        # clear node package
        nodes = objects.NodeList.get_all(ctxt)
        clear_node_flow = uf.Flow('Clean node package')
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            clear_node_flow.add(CephPackageUninstall(
                "Uninstall %s package" % node.id,
                rebind={'node': arg}))
            kwargs[arg] = node
        wf.add(clear_node_flow)

        kwargs.update({
            "ctxt": ctxt,
            'task_info': {}
        })
        taskflow.engines.run(wf, store=kwargs, engine='parallel')
        logger.info("Uninstall ceph flow run success")

    def get_ssh_executor(self, node):
        return SSHExecutor(hostname=str(node.ip_address),
                           password=node.password)


class UninstallService(BaseTask):
    def execute(self, ctxt, task_info):
        super(UninstallService, self).execute(task_info)
        logger.info("Uninstall Service flow start")
        all_node_uninstall_wf = uf.Flow('Nodes Uninstall')
        kwargs = {}
        nodes = objects.NodeList.get_all(ctxt)
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            node_uninstall_flow = lf.Flow('Node Uninstall %s' % node.id)
            node_uninstall_flow.add(DSpaceChronyUninstall(
                "DSpace Chrony Unintall %s" % node.id,
                rebind={'node': arg}))
            node_uninstall_flow.add(DSpaceExpoterUninstall(
                "DSpace Exporter Unintall %s" % node.id,
                rebind={'node': arg}))
            node_uninstall_flow.add(DSpaceAgentUninstall(
                "DSpace Agent Unintall %s" % node.id,
                rebind={'node': arg}))
            all_node_uninstall_wf.add(node_uninstall_flow)
            kwargs[arg] = node
        kwargs.update({
            "ctxt": ctxt,
            'task_info': {}
        })
        taskflow.engines.run(all_node_uninstall_wf,
                             store=kwargs,
                             engine='parallel')
        logger.info("Uninstall Service flow run success")

    def get_ssh_executor(self, node):
        return SSHExecutor(hostname=str(node.ip_address),
                           password=node.password)


class CleanCluster(BaseTask, CleanDataMixin):
    def execute(self, ctxt, task_info):
        """Clean All Include Data

        1. clean volume and volume related
        2. clean pool
        3. clean osd
        4. clean crush rule
        5. clean disk partition
        6. clean disk
        7. clean network
        8. clean node
        9. clean monitor and monitor related
        """
        super(CleanCluster, self).execute(task_info)
        logger.info("Clean database flow start")
        self._clean_volume_mapping(ctxt)
        self._clean_volume_access_path_gateway(ctxt)
        self._clean_volume_client(ctxt)
        self._clean_volume_client_group(ctxt)
        self._clean_volume_gateway(ctxt)
        self._clean_volume_access_path(ctxt)
        self._clean_volume_snapshot(ctxt)
        self._clean_volume(ctxt)
        self._clean_pool(ctxt)
        self._clean_osd(ctxt)
        self._clean_crush_rule(ctxt)
        self._clean_disk_partition(ctxt)
        self._clean_disk(ctxt)
        self._clean_network(ctxt)
        self._clean_node(ctxt)
        self._clean_ceph_config(ctxt)
        self._clean_rpc_service(ctxt)
        self._clean_rack(ctxt)
        self._clean_datacenter(ctxt)
        self._clean_sys_config(ctxt)
        self._clean_email_group(ctxt)
        self._clean_alert_group(ctxt)
        self._clean_alert_rule(ctxt)
        self._clean_alert_log(ctxt)
        self._clean_action_log(ctxt)
        logger.info("Clean database flow success")


def cluster_delete_flow(ctxt, t, clean_ceph):
    # update task
    t.step_num = 4
    t.save()

    # crate task flow
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("Task prepare"))

    if clean_ceph:
        t.step_num += 1
        wf.add(UninstallCeph("Uninstall ceph cluster"))
    wf.add(UninstallService("Uninstall Services"))
    wf.add(CleanCluster("Clean Cluster"))
    wf.add(CompleteTask('Complete'))
    kwargs = {"ctxt": ctxt, 'task_info': {"task": t}}
    taskflow.engines.run(wf, store=kwargs)
    logger.info("Cluster delete flow run success")
