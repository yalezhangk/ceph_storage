import configparser
import logging
import os
import socket
from pathlib import Path

import paramiko
from netaddr import IPAddress
from netaddr import IPNetwork

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.i18n import _
from DSpace.tools.base import SSHExecutor
from DSpace.tools.docker import Docker as DockerTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.service import Service as ServiceTool
from DSpace.tools.system import System as SystemTool
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
        if (IPAddress(storage_cluster_ip_address) not in
                IPNetwork(cluster_cidr)):
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

    @classmethod
    def _collect_node_ip_address(cls, ctxt, node_info):
        admin_ip = node_info.get('admin_ip')
        node_data = {
            'hostname': node_info.get('hostname'),
            'ip_address': admin_ip
        }
        public_cidr = objects.sysconfig.sys_config_get(ctxt, key="public_cidr")
        cluster_cidr = objects.sysconfig.sys_config_get(ctxt,
                                                        key="cluster_cidr")
        for network in node_info.get("networks"):
            ip_addr = network.get("ip_address")
            if (IPAddress(ip_addr) in IPNetwork(public_cidr)):
                node_data['public_ip_address'] = ip_addr
            if (IPAddress(ip_addr) in IPNetwork(cluster_cidr)):
                node_data['cluster_ip_address'] = ip_addr

        return node_data


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

    def get_dspace_repo(self):
        dspace_repo = objects.sysconfig.sys_config_get(
            self.ctxt, "dspace_repo")
        tpl = template.get('dspace.repo.j2')
        repo = tpl.render(dspace_repo=dspace_repo)
        return repo

    def get_ceph_repo(self):
        ceph_repo = objects.sysconfig.sys_config_get(self.ctxt, "ceph_repo")
        tpl = template.get('ceph.repo.j2')
        repo = tpl.render(ceph_repo=ceph_repo)
        return repo

    def get_chrony_conf(self):
        chrony_server = objects.sysconfig.sys_config_get(self.ctxt,
                                                         "chrony_server")
        tpl = template.get('chrony.conf.j2')
        chrony_conf = tpl.render(chrony_server=chrony_server,
                                 ip_address=str(self.node.ip_address))
        return chrony_conf

    def get_dsa_conf(self):
        admin_ip_address = objects.sysconfig.sys_config_get(
            self.ctxt, "admin_ip_address")
        admin_port = objects.sysconfig.sys_config_get(
            self.ctxt, "admin_port")
        agent_port = objects.sysconfig.sys_config_get(
            self.ctxt, "agent_port")

        tpl = template.get('dsa.conf.j2')
        dsa_conf = tpl.render(
            ip_address=str(self.node.ip_address),
            admin_ip_address=str(admin_ip_address),
            admin_port=admin_port,
            agent_port=agent_port,
            node_id=self.node.id,
            cluster_id=self.node.cluster_id
        )
        return dsa_conf

    def chrony_install(self):
        ssh = self.get_ssh_executor()
        # install package
        file_tool = FileTool(ssh)
        file_tool.write("/etc/dspace/chrony.conf",
                        self.get_chrony_conf())

        # run container
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            image="{}/chrony:{}".format(image_namespace, dspace_version),
            privileged=True,
            name="dspace_chrony",
            volumes=[("/etc/dspace", "/etc/dspace"),
                     ("/var/log/dspace", "/var/log/dspace")]
        )

    def chrony_uninstall(self):
        logger.debug("uninstall chrony")
        ssh = self.get_ssh_executor()
        # remove container and image
        docker_tool = DockerTool(ssh)
        docker_tool.stop('dspace_chrony')
        docker_tool.rm('dspace_chrony')
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        docker_tool.image_rm(
            "{}/chrony:{}".format(image_namespace, dspace_version),
            force=True)
        # rm config file
        file_tool = FileTool(ssh)
        file_tool.rm("/etc/dspace/chrony.conf")

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
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        docker_tool.run(
            image="{}/node_exporter:{}".format(image_namespace,
                                               dspace_version),
            privileged=True,
            name="dspace_node_exporter",
            volumes=[("/etc/dspace", "/etc/dspace"),
                     ("/", "/host", "ro,rslave")]
        )

    def node_exporter_uninstall(self):
        logger.debug("uninstall node exporter")
        ssh = self.get_ssh_executor()
        # remove container and image
        docker_tool = DockerTool(ssh)
        docker_tool.stop('dspace_node_exporter')
        docker_tool.rm('dspace_node_exporter')
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        docker_tool.image_rm(
            "{}/node_exporter:{}".format(image_namespace, dspace_version),
            force=True)

    def ceph_mon_install(self):
        logger.debug("install ceph mon")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)

        ceph_auth = objects.CephConfig.get_by_key(
            self.ctxt, 'global', 'auth_cluster_required')
        agent.ceph_mon_create(self.ctxt, ceph_auth=ceph_auth)

    def ceph_mon_uninstall(self, last_mon=False):
        # update ceph.conf
        logger.debug("uninstall ceph mon")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)
        agent.ceph_mon_remove(self.ctxt, last_mon=last_mon)

    def ceph_osd_package_install(self):
        logger.debug("install ceph-osd package on node")
        agent = self.get_agent()
        agent.ceph_osd_package_install(self.ctxt)

    def ceph_osd_package_uninstall(self):
        logger.debug("uninstall ceph-osd package on node")
        agent = self.get_agent()
        agent.ceph_osd_package_uninstall(self.ctxt)

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
        # get global config
        log_dir = objects.sysconfig.sys_config_get(self.ctxt, "log_dir")
        log_dir_container = objects.sysconfig.sys_config_get(
            self.ctxt, "log_dir_container")
        config_dir = objects.sysconfig.sys_config_get(self.ctxt, "config_dir")
        config_dir_container = objects.sysconfig.sys_config_get(
            self.ctxt, "config_dir_container")
        image_name = objects.sysconfig.sys_config_get(self.ctxt, "image_name")
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        dspace_repo = objects.sysconfig.sys_config_get(
            self.ctxt, "dspace_repo")

        # write config
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/dsa.conf".format(config_dir),
                        self.get_dsa_conf())
        file_tool.write("/etc/yum.repos.d/dspace.repo",
                        self.get_dspace_repo())
        file_tool.write("/etc/yum.repos.d/ceph.repo",
                        self.get_ceph_repo())
        # install docker
        package_tool = PackageTool(ssh)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        # start docker
        service_tool = ServiceTool(ssh)
        service_tool.start('docker')
        # load image
        docker_tool = DockerTool(ssh)
        # pull images from repo
        tmp_image = '/tmp/{}'.format(image_name)
        fetch_url = '{}/images/{}'.format(dspace_repo, image_name)
        file_tool.fetch_from_url(tmp_image, fetch_url)
        docker_tool.image_load(tmp_image)
        # run container
        # TODO: remove code_dir
        code_dir = "/root/.local/lib/python3.6/site-packages/DSpace/"
        docker_tool.run(
            image="{}/dspace:{}".format(image_namespace, dspace_version),
            command="dsa",
            name="dsa",
            volumes=[
                (config_dir, config_dir_container),
                (log_dir, log_dir_container),
                ("/", "/host"),
                ("/root/.ssh/", "/root/.ssh", "ro,rslave"),
                ("/opt/t2stor/DSpace/", code_dir)
            ]
        )

    def dspace_agent_uninstall(self):
        logger.debug("uninstall chrony")
        ssh = self.get_ssh_executor()
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(self.ctxt,
                                                          "dspace_version")
        # remove container and image
        docker_tool = DockerTool(ssh)
        docker_tool.stop('dsa')
        docker_tool.rm('dsa')
        docker_tool.image_rm(
            "{}/dspace:{}".format(image_namespace, dspace_version),
            force=True)
        # rm config file
        file_tool = FileTool(ssh)
        file_tool.rm("/etc/dspace")

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

    def node_get_infos(self):
        try:
            ssh = self.get_ssh_executor()
        except paramiko.ssh_exception.SSHException:
            raise exc.InvalidInput(_('SSH Error'))
        sys_tool = SystemTool(ssh)
        node_infos = sys_tool.get_node_baseinfo()
        return node_infos

    def check_port(self, port):
        res = True
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((str(self.node.ip_address), port))
            logger.info("port %d has been used" % port)
            res = False
        except Exception:
            pass
        finally:
            sock.close()
        return res

    def check_network(self, address):
        res = True
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((address, 22))
        except Exception:
            logger.info("connect to %s error" % address)
            res = False
        finally:
            sock.close()
        return res

    def check_selinux(self):
        ssh = self.get_ssh_executor()
        sys_tool = SystemTool(ssh)
        result = sys_tool.check_selinux()
        return result

    def check_package(self, pkg_name):
        ssh = self.get_ssh_executor()
        sys_tool = SystemTool(ssh)
        result = sys_tool.check_package(pkg_name)
        return result
