#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

import taskflow.engines
from oslo_log import log as logging
from taskflow.patterns import linear_flow as lf
from taskflow.types.failure import Failure

from DSpace import context
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.objects import fields as s_fields
from DSpace.taskflows.base import BaseTask
from DSpace.taskflows.base import CompleteTask
from DSpace.taskflows.base import PrepareTask
from DSpace.taskflows.node import DSpaceAgentInstall
from DSpace.taskflows.node import DSpaceChronyInstall
from DSpace.taskflows.node import DSpaceExpoterInstall
from DSpace.taskflows.node import InstallDocker
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

    def revert(self, task_info, result, flow_failures):
        if isinstance(result, Failure):
            return
        for node in result:
            node.status = s_fields.NodeStatus.ERROR
            node.save()


class InstallService(BaseTask):
    def execute(self, ctxt, nodes, task_info):
        super(InstallService, self).execute(task_info)
        wf = lf.Flow('TaskFlow')
        kwargs = {}
        for node in nodes:
            node.executer = self.get_ssh_executor(node)
            arg = "node-%s" % node.id
            wf.add(InstallDocker(
                "Intall Docker %s" % node.id,
                rebind={'node': arg}))
            wf.add(DSpaceChronyInstall(
                "DSpace Chrony Intall %s" % node.id,
                rebind={'node': arg}))
            wf.add(DSpaceExpoterInstall(
                "DSpace Exporter Intall %s" % node.id,
                rebind={'node': arg}))
            wf.add(DSpaceAgentInstall(
                "DSpace Agent Intall %s" % node.id,
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


class MakeNodeActive(BaseTask):
    def execute(self, ctxt, nodes, task_info):
        super(MakeNodeActive, self).execute(task_info)
        for node in nodes:
            node.status = s_fields.NodeStatus.ACTIVE
            node.save()
        return nodes

    def revert(self, task_info, result, flow_failures):
        pass


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

    def revert(self, task_info, result, flow_failures):
        pass

    def _update_osd(self, ctxt, osd_info, node):
        osd = objects.Osd(
            ctxt, node_id=node.id,
            fsid="xxx",
            osd_id=osd_info.get('osd_id'),
            type=osd_info.get('type', s_fields.OsdType.BLUESTORE),
            disk_type='hdd',
            status=s_fields.OsdStatus.ACTIVE
        )
        osd.create()


class UpdateIncludeFinish(BaseTask):
    def execute(self, ctxt, task_info):
        """Mark Include Finish"""
        super(UpdateIncludeFinish, self).execute(task_info)
        objects.sysconfig.sys_config_set(
            ctxt, 'import_task_id', -1
        )

    def revert(self, task_info, result, flow_failures):
        pass


def include_flow(ctxt, t, datas):
    # update task
    t.step_num = 5
    t.save()

    # crate task flow
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("Task prepare"))
    wf.add(CreateDB("Database add info"))
    wf.add(InstallService("Install Services"))
    wf.add(MakeNodeActive("Mark Node Active"))
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