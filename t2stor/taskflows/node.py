#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import logging
import os

from t2stor import objects
from t2stor.admin.genconf import get_agent_conf
from t2stor.admin.genconf import yum_repo
from t2stor.agent.client import AgentClientManager
from t2stor.tools.base import Executor
from t2stor.tools.base import SSHExecutor
from t2stor.tools.docker import Docker as DockerTool
from t2stor.tools.file import File as FileTool
from t2stor.tools.package import Package as PackageTool
from t2stor.tools.service import Service as ServiceTool
from t2stor.utils import template

logger = logging.getLogger(__name__)


class NodeTask(object):
    ctxt = None
    node = None
    host_perfix = None

    def __init__(self, ctxt, node, host_prefix=None):
        self.host_prefix = host_prefix
        self.ctxt = ctxt
        self.node = node

    def _wapper(self, path):
        if not self.host_prefix:
            return path
        if path[0] == os.path.sep:
            path = path[1:]
        return os.path.join(self.host_prefix, path)

    def get_ssh_executor(self):
        return SSHExecutor(hostname=str(self.node.ip_address),
                           password=self.node.password)

    def get_agent(self):
        client = AgentClientManager(
            self.ctxt, cluster_id=self.ctxt.cluster_id
        ).get_client(self.node.id)
        return client

    def get_yum_repo(self):
        repo_url = objects.sysconfig.sys_config_get("repo_url")
        tpl = template.get('yum.repo.j2')
        repo = tpl.render(repo_baseurl=repo_url)
        return repo

    def chrony_install(self):
        ssh = self.get_ssh_executor()
        # install package
        package_tool = PackageTool(ssh)
        package_tool.install(["chrony"])
        # write config
        file_tool = FileTool(ssh)
        file_tool.write("/etc/chrony.conf", get_agent_conf(None))
        # start service
        service_tool = ServiceTool(ssh)
        service_tool.restart('chronyd')

    def chrony_uninstall(self):
        ssh = self.get_ssh_executor()
        package_tool = PackageTool(ssh)
        package_tool.uninstall(["chrony"])

    def chrony_update(self):
        pass
        # TODO 更新时间同步服务器配置
        # file_tool = FileTool(ssh)
        # file_tool.write("/etc/chrony.conf", get_agent_conf(None))

    def chrony_restart(self):
        ssh = self.get_ssh_executor()
        service_tool = ServiceTool(ssh)
        service_tool.restart('chronyd')

    def ceph_mon_install(self):
        # install package
        # write ceph.conf
        # start service
        pass

    def ceph_mon_uninstall(self):
        # update ceph.conf
        # stop service
        # uninstall package
        pass

    def ceph_osd_install(self, osd):
        # write ceph.conf
        logger.debug("write config")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)

        # TODO: key
        logger.debug("osd create on node")
        osd = agent.ceph_osd_create(self.ctxt, osd)

        return osd

    def ceph_osd_uninstall(self, osd):
        logger.debug("osd destroy on node")
        agent = self.get_agent()
        osd = agent.ceph_osd_destroy(self.ctxt, osd)
        return osd

    def ceph_rgw_install(self):
        # write ceph.conf
        # start service
        pass

    def ceph_rgw_uninstall(self):
        # update ceph.conf
        # stop service
        pass

    def ceph_igw_install(self):
        """Ceph ISCSI gateway install"""
        pass

    def ceph_igw_uninstall(self):
        """Ceph ISCSI gateway uninstall"""
        pass

    def t2stor_agent_install(self, ip_address, password):
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        # create config
        file_tool = FileTool(ssh_client)
        file_tool.mkdir("/etc/t2stor")
        file_tool.write("/etc/t2stor/agent.conf", get_agent_conf(ip_address))
        file_tool.write("/etc/yum.repos.d/yum.repo", yum_repo)
        # install docker
        package_tool = PackageTool(ssh_client)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        # start docker
        service_tool = ServiceTool(ssh_client)
        service_tool.start('docker')
        # load image
        docker_tool = DockerTool(ssh_client)
        docker_tool.image_load("/opt/t2stor/repo/files/t2stor.tar")
        # run container
        docker_tool.run(
            image="t2stor/t2stor:v2.3",
            command="agent",
            name="t2stor_portal",
            volumes=[("/etc/t2stor", "/etc/t2stor")]
        )

    def t2stor_agent_uninstall(self):
        # stop agent
        # rm config file
        # rm image
        pass

    def ceph_config_update(self, values):
        path = self._wapper('/etc/ceph/ceph.conf')
        configer = configparser.ConfigParser()
        configer.read(path)
        if not configer.has_section(values['group']):
            configer.add_section(values['group'])
        configer.set(values['group'], values['key'], str(values['value']))
        configer.write(open(path, 'w'))
