import configparser
import logging
import os
from pathlib import Path

import paramiko
from netaddr import IPAddress
from netaddr import IPNetwork

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.tools.base import SSHExecutor
from DSpace.tools.docker import Docker as DockerTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.service import Service as ServiceTool
from DSpace.utils import template

logger = logging.getLogger(__name__)


class NodeMixin(object):
    @classmethod
    def _check_node_ip_address(cls, ctxt, data):
        ip_address = data.get('ip_address')
        storage_cluster_ip_address = data.get('storage_cluster_ip_address')
        storage_public_ip_address = data.get('storage_public_ip_address')
        admin_cidr = objects.sysconfig.sys_config_get(ctxt, key="admin_cidr")
        public_cidr = objects.sysconfig.sys_config_get(ctxt, key="public_cidr")
        cluster_cidr = objects.sysconfig.sys_config_get(ctxt,
                                                        key="cluster_cidr")
        if not all([ip_address,
                    storage_cluster_ip_address,
                    storage_public_ip_address]):
            raise exc.Invalid('ip_address,storage_cluster_ip_address,'
                              'storage_public_ip_address is required')

        if objects.NodeList.get_all(ctxt, filters={"ip_address": ip_address}):
            raise exc.Invalid("ip_address already exists!")
        if IPAddress(ip_address) not in IPNetwork(admin_cidr):
            raise exc.Invalid("admin ip not in admin cidr ({})"
                              "".format(admin_cidr))
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "storage_cluster_ip_address": storage_cluster_ip_address
            }
        ):
            raise exc.Invalid("storage_cluster_ip_address already exists!")
        if IPAddress(storage_cluster_ip_address)not in IPNetwork(cluster_cidr):
            raise exc.Invalid("cluster ip not in cluster cidr ({})"
                              "".format(cluster_cidr))
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "storage_public_ip_address": storage_public_ip_address
            }
        ):
            raise exc.Invalid("storage_public_ip_address already exists!")
        if (IPAddress(storage_public_ip_address) not in
                IPNetwork(public_cidr)):
            raise exc.Invalid("public ip not in public cidr ({})"
                              "".format(public_cidr))


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

    def get_ssh_key(self):
        home = str(Path.home())
        pk = paramiko.RSAKey.from_private_key(open('%s/.ssh/id_rsa' % home))
        return pk

    def get_sftp_client(self):
        ip_addr = str(self.node.ip_address)
        password = self.node.password
        p_key = self.get_ssh_key()
        transport = paramiko.Transport((ip_addr, 22))
        transport.connect(username='root', password=password, pkey=p_key)
        sftp_client = paramiko.SFTPClient.from_transport(transport)
        return sftp_client, transport

    def get_agent(self):
        client = AgentClientManager(
            self.ctxt, cluster_id=self.ctxt.cluster_id
        ).get_client(self.node.id)
        return client

    def get_yum_repo(self):
        repo_url = objects.sysconfig.sys_config_get(self.ctxt, "repo_url")
        tpl = template.get('yum.repo.j2')
        repo = tpl.render(repo_baseurl=repo_url)
        return repo

    def get_chrony_conf(self):
        chrony_server = objects.sysconfig.sys_config_get(self.ctxt,
                                                         "chrony_server")
        tpl = template.get('chrony.conf.j2')
        chrony_conf = tpl.render(chrony_server=chrony_server,
                                 ip_address=str(self.node.ip_address))
        return chrony_conf

    def get_agent_conf(self):
        admin_ip_address = objects.sysconfig.sys_config_get(
            self.ctxt, "admin_ip_address")
        api_port = objects.sysconfig.sys_config_get(
            self.ctxt, "api_port")
        websocket_port = objects.sysconfig.sys_config_get(
            self.ctxt, "websocket_port")
        admin_port = objects.sysconfig.sys_config_get(
            self.ctxt, "admin_port")
        agent_port = objects.sysconfig.sys_config_get(
            self.ctxt, "agent_port")
        database_user = objects.sysconfig.sys_config_get(
            self.ctxt, "database_user")
        database_password = objects.sysconfig.sys_config_get(
            self.ctxt, "database_password")

        tpl = template.get('agent.conf.j2')
        agent_conf = tpl.render(
            ip_address=str(self.node.ip_address),
            admin_ip_address=admin_ip_address,
            api_port=api_port,
            websocket_port=websocket_port,
            admin_port=admin_port,
            agent_port=agent_port,
            node_id=self.node.id,
            cluster_id=self.node.cluster_id,
            database_user=database_user,
            database_password=database_password
        )
        return agent_conf

    def chrony_install(self):
        ssh = self.get_ssh_executor()
        # install package
        file_tool = FileTool(ssh)
        file_tool.write("/etc/dspace/chrony.conf",
                        self.get_chrony_conf())

        # run container
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            image="dspace/chrony:v2.3",
            privileged=True,
            name="dspace_chrony",
            volumes=[("/etc/dspace", "/etc/dspace"),
                     ("/var/log/dspace", "/var/log/dspace")]
        )

    def chrony_uninstall(self):
        pass

    def chrony_update(self):
        ssh = self.get_ssh_executor()
        # install package
        file_tool = FileTool(ssh)
        file_tool.write("/etc/dspace/chrony.conf",
                        self.get_chrony_conf())

        # restart container
        docker_tool = DockerTool(ssh)
        docker_tool.restart('dspace_chrony')

    def node_exporter_install(self):
        ssh = self.get_ssh_executor()
        # run container
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            image="dspace/node_exporter:v2.3",
            privileged=True,
            name="dspace_node_exporter",
            volumes=[("/etc/dspace", "/etc/dspace"),
                     ("/", "/host", "ro,rslave")]
        )

    def node_exporter_uninstall(self):
        pass

    def node_exporter_restart(self):
        pass

    def ceph_mon_install(self):
        logger.debug("install ceph mon")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)

        # TODO: key
        logger.debug("mon install on node")
        ceph_auth = objects.CephConfig.get_by_key(
            self.ctxt, 'global', 'auth_cluster_required')
        agent.ceph_mon_create(self.ctxt, ceph_auth=ceph_auth)

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

    def dspace_agent_install(self):
        ssh = self.get_ssh_executor()
        # create config
        file_tool = FileTool(ssh)
        file_tool.mkdir("/etc/dspace")
        file_tool.write("/etc/dspace/agent.conf",
                        self.get_agent_conf())
        file_tool.write("/etc/yum.repos.d/yum.repo",
                        self.get_yum_repo())
        # install docker
        package_tool = PackageTool(ssh)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        # start docker
        service_tool = ServiceTool(ssh)
        service_tool.start('docker')
        # load image
        docker_tool = DockerTool(ssh)
        docker_tool.image_load("/opt/dspace/repo/images/dspace-base-v2.3.tar")
        # run container
        docker_tool.run(
            image="dspace/dspace:v2.3",
            command="agent",
            name="dspace_portal",
            volumes=[("/etc/dspace", "/etc/dspace")]
        )

    def dspace_agent_uninstall(self):
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

    def pull_logfile(self, directory, filename, LOCAL_LOGFILE_DIR):
        try:
            sftp_client, transport = self.get_sftp_client()
            sftp_client.get('{}{}'.format(directory, filename),
                            '{}{}'.format(LOCAL_LOGFILE_DIR, filename))
            # 将node140上的/var/log/ceph/xx.log下载到admin201.131上
            transport.close()
        except Exception as e:
            logger.error('pull_logfile error,error:{}'.format(e))
            raise exc.CephException(message='pull log_file error,reason:'
                                            '{}'.format(str(e)))
