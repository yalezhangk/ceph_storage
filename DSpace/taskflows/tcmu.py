# Last Update:2020-02-11 23:32:37

import logging
from os import path

import taskflow
from taskflow.patterns import linear_flow as lf

from DSpace import exception as exc
from DSpace import objects
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.base import BaseTask
from DSpace.taskflows.node import ContainerUninstallMixin
from DSpace.taskflows.node import NodeTask
from DSpace.taskflows.node import ServiceMixin
from DSpace.tools.docker import Docker as DockerTool
from DSpace.tools.file import File as FileTool
from DSpace.utils import template

logger = logging.getLogger(__name__)


class DSpaceTcmuInstall(BaseTask, NodeTask, ServiceMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceTcmuInstall, self).execute(task_info)
        ssh = node.executer
        image_namespace = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.IMAGE_NAMESPACE)
        dspace_version = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DSPACE_VERSION)
        dsa_lib_dir = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DSA_LIB_DIR)
        docker_registry = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DOCKER_REGISTRY)
        container_prefix = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.CONTAINER_PREFIX)

        # run container
        volumes = [
            ("/lib/modules/", "/lib/modules/"),
            (dsa_lib_dir, "/etc/ceph"),
            ("/sys", "/sys"),
            ("/dev", "/dev"),
            ("/run", "/run"),
        ]
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            image="{}/tcmu_runner:{}".format(image_namespace, dspace_version),
            command="tcmu-runner",
            privileged=True,
            name="{}_tcmu_runner".format(container_prefix),
            volumes=volumes,
            registry=docker_registry,
        )
        self.service_create(ctxt, "TCMU", node.id, "role_block_gateway")


class DSpaceTcmuUninstall(BaseTask, ContainerUninstallMixin, ServiceMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceTcmuUninstall, self).execute(task_info)
        container_prefix = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.CONTAINER_PREFIX)
        ssh = node.executer
        file_tool = FileTool(ssh)
        docker_tool = DockerTool(ssh)
        container_name = '{}_{}'.format(container_prefix, "tcmu_runner")
        docker_tool.rm(container_name, force=True)
        file_tool.rm("/etc/dbus-1/system.d/tcmu-runner.conf")
        self.service_delete(ctxt, "TCMU", node.id)


class DSpaceTcmuRemove(BaseTask, ContainerUninstallMixin, ServiceMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceTcmuRemove, self).execute(task_info)
        ssh = node.executer
        image_namespace = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.IMAGE_NAMESPACE)
        docker_image_ignore = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DOCKER_IMAGE_IGNORE)
        dspace_version = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.DSPACE_VERSION)
        docker_tool = DockerTool(ssh)
        try:
            if docker_image_ignore:
                return
            docker_tool.image_rm(
                "{}/{}:{}".format(image_namespace, "tcmu_runner",
                                  dspace_version))
            logger.info("remove tcmu_runner image success")
        except exc.StorException as e:
            logger.exception("remove tcmu_runner image failed, %s", e)


class TcmuTask(NodeTask):
    ctxt = None
    node = None

    def __init__(self, ctxt, node):
        self.ctxt = ctxt
        self.node = node
        super(TcmuTask, self).__init__(ctxt, node)

    def _get_configs(self, ctxt):
        configs = {
            "global": objects.ceph_config.ceph_config_group_get(
                ctxt, "global")
        }
        return configs

    def tcmu_config_set(self):
        enable_cephx = objects.sysconfig.sys_config_get(
            self.ctxt, key=ConfigKey.ENABLE_CEPHX)
        dsa_lib_dir = objects.sysconfig.sys_config_get(
            self.ctxt, ConfigKey.DSA_LIB_DIR)
        # write ceph config
        configs = self._get_configs(self.ctxt)
        logger.info("config set, get configs: %s", configs)
        agent = self.get_agent()
        ceph_config_path = path.join(dsa_lib_dir, "ceph.conf")
        agent.ceph_config_set(self.ctxt, configs, ceph_config_path)
        if enable_cephx:
            self.init_admin_key(dsa_lib_dir)

    def tcmu_install(self):
        self.tcmu_config_set()
        ssh = self.get_ssh_executor()
        wf = lf.Flow('DSpace tcmu Install')
        tpl = template.get('tcmu-runner.conf')
        file_tool = FileTool(ssh)
        file_content = tpl.render()
        file_tool.write("/etc/dbus-1/system.d/tcmu-runner.conf", file_content)
        wf.add(DSpaceTcmuInstall('DSpace tcmu Install'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def tcmu_uninstall(self):
        logger.info("uninstall tcmu")
        wf = lf.Flow('DSpace tcmu Uninstall')
        wf.add(DSpaceTcmuUninstall('DSpace tcmu Uninstall'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def tcmu_remove_image(self):
        logger.info("remove tcmu image")
        wf = lf.Flow('DSpace tcmu Remove Image')
        wf.add(DSpaceTcmuRemove('DSpace tcmu Remove Image'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })
