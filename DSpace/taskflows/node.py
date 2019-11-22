import configparser
import logging
import os
import socket
import time
from pathlib import Path

import paramiko
import taskflow.engines
from grpc._channel import _Rendezvous
from netaddr import IPAddress
from netaddr import IPNetwork
from taskflow.patterns import linear_flow as lf

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSA.client import AgentClientManager
from DSpace.i18n import _
from DSpace.taskflows.base import BaseTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool
from DSpace.tools.docker import Docker as DockerTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.probe import ProbeTool
from DSpace.tools.service import Service as ServiceTool
from DSpace.tools.system import System as SystemTool
from DSpace.utils import template

logger = logging.getLogger(__name__)
CODE_DIR_CONTAINER = "/root/.local/lib/python3.6/site-packages/DSpace/"


class NodeMixin(object):
    @classmethod
    def _check_node_ip_address(cls, ctxt, data):
        ip_address = data.get('ip_address')
        cluster_ip = data.get('cluster_ip')
        public_ip = data.get('public_ip')
        admin_cidr = objects.sysconfig.sys_config_get(ctxt, key="admin_cidr")
        public_cidr = objects.sysconfig.sys_config_get(ctxt, key="public_cidr")
        cluster_cidr = objects.sysconfig.sys_config_get(ctxt,
                                                        key="cluster_cidr")
        if not all([ip_address,
                    cluster_ip,
                    public_ip]):
            raise exc.Invalid('ip_address,cluster_ip,'
                              'public_ip is required')
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "cluster_id": "*",
                "ip_address": ip_address
            }
        ):
            raise exc.Invalid("ip_address already exists!")
        if IPAddress(ip_address) not in IPNetwork(admin_cidr):
            raise exc.Invalid("admin ip not in admin cidr ({})"
                              "".format(admin_cidr))
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "cluster_ip": cluster_ip
            }
        ):
            raise exc.Invalid("cluster_ip already exists!")
        if (IPAddress(cluster_ip) not in
                IPNetwork(cluster_cidr)):
            raise exc.Invalid("cluster ip not in cluster cidr ({})"
                              "".format(cluster_cidr))
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "public_ip": public_ip
            }
        ):
            raise exc.Invalid("public_ip already exists!")
        if (IPAddress(public_ip) not in
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
                node_data['public_ip'] = ip_addr
            if (IPAddress(ip_addr) in IPNetwork(cluster_cidr)):
                node_data['cluster_ip'] = ip_addr

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

    def chrony_install(self):
        wf = lf.Flow('DSpace Chrony Install')
        wf.add(DSpaceChronyInstall('DSpace Chrony Install'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def chrony_uninstall(self):
        logger.info("uninstall chrony")
        wf = lf.Flow('DSpace Chrony Uninstall')
        wf.add(DSpaceChronyUninstall('DSpace Chrony Uninstall'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def chrony_update(self):
        ssh = self.get_ssh_executor()
        # install package
        config_dir = objects.sysconfig.sys_config_get(self.ctxt, "config_dir")
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        file_tool = FileTool(ssh)
        file_tool.write("{}/chrony.conf".format(config_dir),
                        self.get_chrony_conf())

        # restart container
        docker_tool = DockerTool(ssh)
        docker_tool.restart('{}_chrony'.format(image_namespace))

    def node_exporter_install(self):
        logger.info("Install node exporter for %s", self.node.hostname)
        wf = lf.Flow('DSpace Exporter Install')
        wf.add(DSpaceExpoterInstall('DSpace Exporter Install'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def node_exporter_uninstall(self):
        logger.info("uninstall node exporter")
        # remove container and image
        wf = lf.Flow('DSpace Exporter Uninstall')
        wf.add(DSpaceExpoterUninstall('DSpace Exporter Uninstall'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def ceph_mon_install(self):
        logger.info("install ceph mon")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)

        ceph_auth = objects.CephConfig.get_by_key(
            self.ctxt, 'global', 'auth_cluster_required')
        agent.ceph_mon_create(self.ctxt, ceph_auth=ceph_auth)

    def ceph_mon_uninstall(self, last_mon=False):
        # update ceph.conf
        logger.info("uninstall ceph mon")
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)
        agent.ceph_mon_remove(self.ctxt, last_mon=last_mon)

    def ceph_osd_package_install(self):
        logger.info("install ceph-osd package on node")
        agent = self.get_agent()
        agent.ceph_osd_package_install(self.ctxt)

    def ceph_osd_package_uninstall(self):
        logger.info("uninstall ceph-osd package on node")
        agent = self.get_agent()
        agent.ceph_osd_package_uninstall(self.ctxt)

    def ceph_package_uninstall(self):
        logger.info("uninstall ceph-common package on node")
        try:
            agent = self.get_agent()
            agent.ceph_package_uninstall(self.ctxt)
        except Exception as e:
            logger.warning("uninstall ceph package failed: %s", e)

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
        logger.info("osd destroy on node")
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

    def mount_bgw(self, ctxt, access_path, node):
        """mount ISCSI gateway"""
        agent = self.get_agent()
        agent.mount_bgw(ctxt, access_path, node)

    def unmount_bgw(self, ctxt, access_path):
        """unmount ISCSI gateway"""
        agent = self.get_agent()
        agent.unmount_bgw(ctxt, access_path)

    def bgw_set_chap(self, ctxt, access_path, chap_enable,
                     username, password):
        agent = self.get_agent()
        agent.bgw_set_chap(ctxt, self.node, access_path,
                           chap_enable, username, password)

    def bgw_create_mapping(self, ctxt, access_path, volume_client, volumes):
        agent = self.get_agent()
        agent.bgw_create_mapping(
            ctxt, self.node, access_path, volume_client, volumes)

    def bgw_remove_mapping(self, ctxt, access_path, volume_client, volumes):
        agent = self.get_agent()
        agent.bgw_remove_mapping(
            ctxt, self.node, access_path, volume_client, volumes)

    def bgw_add_volume(self, ctxt, access_path, volume_client, volumes):
        agent = self.get_agent()
        agent.bgw_add_volume(
            ctxt, self.node, access_path, volume_client, volumes)

    def bgw_remove_volume(self, ctxt, access_path, volume_client, volumes):
        agent = self.get_agent()
        agent.bgw_remove_volume(
            ctxt, self.node, access_path, volume_client, volumes)

    def bgw_change_client_group(self, ctxt, access_path, volumes,
                                volume_clients, new_volume_clients):
        agent = self.get_agent()
        agent.bgw_change_client_group(ctxt, access_path, volumes,
                                      volume_clients, new_volume_clients)

    def bgw_set_mutual_chap(self, ctxt, access_path, volume_clients,
                            mutual_chap_enable, mutual_username,
                            mutual_password):
        agent = self.get_agent()
        agent.bgw_set_mutual_chap(
            ctxt, access_path, volume_clients, mutual_chap_enable,
            mutual_username, mutual_password)

    def dspace_agent_install(self):
        wf = lf.Flow('DSpace Agent Install')
        wf.add(InstallDocker('Install Docker'))
        wf.add(DSpaceAgentInstall('DSpace Agent Install'))
        wf.add(InstallCephRepo('Install Ceph Repo'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

    def dspace_agent_uninstall(self):
        logger.info("uninstall agent")
        wf = lf.Flow('DSpace Agent Uninstall')
        wf.add(DSpaceAgentUninstall('DSpace Agent Uninstall'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

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
        except Exception as e:
            raise exc.InvalidInput(_('SSH Error: %s' % e))
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

    def check_firewall(self):
        ssh = self.get_ssh_executor()
        sys_tool = SystemTool(ssh)
        result = sys_tool.check_firewall()
        return result

    def get_ceph_service(self):
        ssh = self.get_ssh_executor()
        sys_tool = SystemTool(ssh)
        services = []
        for i in ["ceph-mon", "ceph-osd", "ceph-mgr", "ceph-radosgw"]:
            res = sys_tool.get_process_list(i)
            if res:
                services.append(i)
        return services

    def check_ceph_is_installed(self):
        ssh = self.get_ssh_executor()
        ceph_tool = CephTool(ssh)
        # 判断是否有残留环境
        result = ceph_tool.check_ceph_is_installed()
        sys_tool = SystemTool(ssh)
        result |= sys_tool.check_package('ceph')
        file_tool = FileTool(ssh)
        result |= file_tool.exist('/etc/ceph')
        result |= file_tool.exist('/var/lib/ceph')
        return result

    def probe_node_services(self):
        ssh = self.get_ssh_executor()
        probe_tool = ProbeTool(ssh)
        result = probe_tool.probe_node_services()
        return result

    def prometheus_target_config(self, action, service, port=None, path=None):
        logger.info("Config from prometheus target file: %s, %s, %s",
                    service, self.node.ip_address, self.node.hostname)
        if not path:
            path = objects.sysconfig.sys_config_get(
                self.ctxt, "config_dir_container")
        if not port:
            if service == 'node_exporter':
                port = objects.sysconfig.sys_config_get(
                    self.ctxt, "node_exporter_port")
            if service == "mgr":
                port = '9283'
        ip = self.node.ip_address
        hostname = self.node.hostname

        admin_node = objects.NodeList.get_all(
            self.ctxt, filters={'role_admin': 1})
        for node in admin_node:
            client = AgentClientManager(
                self.ctxt, cluster_id=self.node.cluster_id
            ).get_client(node_id=node.id)
            if action == "add":
                logger.info("Add to %s prometheus target file: %s, %s",
                            node.hostname, ip, port)
                client.prometheus_target_add(
                    self.ctxt, ip=str(ip), port=port,
                    hostname=hostname,
                    path=path + '/prometheus/targets/targets.json')
            if action == "remove":
                logger.info("Remove from %s prometheus target file: %s, %s",
                            node.hostname, ip, port)
                client.prometheus_target_remove(
                    self.ctxt, ip=str(ip), port=port,
                    hostname=hostname,
                    path=path + '/prometheus/targets/targets.json')


class ContainerUninstallMixin(object):
    def _node_remove_container(self, ctxt, ssh, container_name, image_name):
        image_namespace = objects.sysconfig.sys_config_get(
            ctxt, "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(
            ctxt, "dspace_version")
        docker_tool = DockerTool(ssh)
        container_name = '{}_{}'.format(image_namespace, container_name)
        status = docker_tool.status(container_name)
        if status == 'active':
            docker_tool.stop(container_name)
            docker_tool.rm(container_name)
        if status == 'inactive':
            docker_tool.rm(container_name)
        try:
            docker_tool.image_rm(
                "{}/{}:{}".format(image_namespace, image_name, dspace_version))
        except exc.StorException as e:
            logger.warning("remove %s image failed, %s", image_name, e)


class InstallCephRepo(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(InstallCephRepo, self).execute(task_info)
        ssh = node.executer
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("/etc/yum.repos.d/ceph.repo",
                        self.get_ceph_repo(ctxt))

    def get_ceph_repo(self, ctxt):
        ceph_repo = objects.sysconfig.sys_config_get(ctxt, "ceph_repo")
        tpl = template.get('ceph.repo.j2')
        repo = tpl.render(ceph_repo=ceph_repo)
        return repo


class InstallDocker(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(InstallDocker, self).execute(task_info)
        ssh = node.executer
        # write config
        file_tool = FileTool(ssh)
        file_tool.write("/etc/yum.repos.d/dspace.repo",
                        self.get_dspace_repo(ctxt))
        # install docker
        package_tool = PackageTool(ssh)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        # start docker
        service_tool = ServiceTool(ssh)
        service_tool.enable('docker')
        service_tool.start('docker')

        # load image
        docker_tool = DockerTool(ssh)
        rc = False
        for i in range(7):
            rc = docker_tool.available()
            if rc:
                break
            else:
                time.sleep(2 ** i)
        if not rc:
            logger.error("Docker service not available")
            raise exc.ProgrammingError("Docker service not available")

        # pull images from repo
        dspace_repo = objects.sysconfig.sys_config_get(
            ctxt, "dspace_repo")
        image_name = objects.sysconfig.sys_config_get(ctxt, "image_name")
        tmp_image = '/tmp/{}'.format(image_name)
        fetch_url = '{}/images/{}'.format(dspace_repo, image_name)
        file_tool.fetch_from_url(tmp_image, fetch_url)
        docker_tool.image_load(tmp_image)

    def get_dspace_repo(self, ctxt):
        dspace_repo = objects.sysconfig.sys_config_get(
            ctxt, "dspace_repo")
        tpl = template.get('dspace.repo.j2')
        repo = tpl.render(dspace_repo=dspace_repo)
        return repo


class DSpaceAgentUninstall(BaseTask, ContainerUninstallMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceAgentUninstall, self).execute(task_info)
        ssh = node.executer
        self._node_remove_container(ctxt, ssh, "dsa", "dspace")
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        # rm config file
        file_tool = FileTool(ssh)
        file_tool.rm("{}/dsa.conf".format(config_dir))


class DSpaceAgentInstall(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(DSpaceAgentInstall, self).execute(task_info)

        ssh = node.executer
        # get global config
        log_dir = objects.sysconfig.sys_config_get(ctxt, "log_dir")
        log_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "log_dir_container")
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        config_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "config_dir_container")
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(ctxt,
                                                          "dspace_version")

        # write config
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/dsa.conf".format(config_dir),
                        self.get_dsa_conf(ctxt, node))
        # run container
        # TODO: remove code_dir
        code_dir = objects.sysconfig.sys_config_get(ctxt, "dspace_dir")
        debug_mode = objects.sysconfig.sys_config_get(ctxt, "debug_mode")
        volumes = [
            (config_dir, config_dir_container),
            (log_dir, log_dir_container),
            ("/", "/host"),
            ("/sys", "/sys"),
            ("/root/.ssh/", "/root/.ssh", "ro,rslave")
        ]
        if debug_mode == "yes":
            volumes.append((code_dir, CODE_DIR_CONTAINER))
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            name="{}_dsa".format(image_namespace),
            image="{}/dspace:{}".format(image_namespace, dspace_version),
            command="dsa",
            privileged=True,
            volumes=volumes
        )

    def get_dsa_conf(self, ctxt, node):
        admin_ip_address = objects.sysconfig.sys_config_get(
            ctxt, "admin_ip_address")
        admin_port = objects.sysconfig.sys_config_get(
            ctxt, "admin_port")
        agent_port = objects.sysconfig.sys_config_get(
            ctxt, "agent_port")

        tpl = template.get('dsa.conf.j2')
        dsa_conf = tpl.render(
            ip_address=str(node.ip_address),
            admin_ip_address=str(admin_ip_address),
            admin_port=admin_port,
            agent_port=agent_port,
            node_id=node.id,
            cluster_id=node.cluster_id
        )
        return dsa_conf


class DSpaceChronyUninstall(BaseTask, ContainerUninstallMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceChronyUninstall, self).execute(task_info)
        ssh = node.executer
        # remove container and image
        self._node_remove_container(ctxt, ssh, "chrony", "chrony")
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        # rm config file
        file_tool = FileTool(ssh)
        try:
            file_tool.rm("{}/chrony.conf".format(config_dir))
        except exc.StorException as e:
            logger.warning("remove chrony config failed, %s", e)


class DSpaceChronyInstall(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(DSpaceChronyInstall, self).execute(task_info)

        ssh = node.executer
        # install package
        log_dir = objects.sysconfig.sys_config_get(ctxt, "log_dir")
        log_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "log_dir_container")
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        config_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "config_dir_container")
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(ctxt,
                                                          "dspace_version")
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/chrony.conf".format(config_dir),
                        self.get_chrony_conf(ctxt, node.ip_address))

        # run container
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            image="{}/chrony:{}".format(image_namespace, dspace_version),
            privileged=True,
            name="{}_chrony".format(image_namespace),
            volumes=[(config_dir, config_dir_container),
                     (log_dir, log_dir_container)]
        )

    def get_chrony_conf(self, ctxt, ip):
        chrony_server = objects.sysconfig.sys_config_get(ctxt,
                                                         "chrony_server")
        tpl = template.get('chrony.conf.j2')
        chrony_conf = tpl.render(chrony_server=chrony_server,
                                 ip_address=str(ip))
        return chrony_conf


class DSpaceExpoterUninstall(BaseTask, ContainerUninstallMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceExpoterUninstall, self).execute(task_info)

        ssh = node.executer
        self._node_remove_container(ctxt, ssh, "node_exporter",
                                    "node_exporter")


class DSpaceExpoterInstall(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(DSpaceExpoterInstall, self).execute(task_info)

        ssh = node.executer
        # run container
        docker_tool = DockerTool(ssh)
        config_dir = objects.sysconfig.sys_config_get(ctxt, "config_dir")
        config_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "config_dir_container")
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(ctxt,
                                                          "dspace_version")
        node_exporter_port = objects.sysconfig.sys_config_get(
            ctxt, "node_exporter_port")
        docker_tool.run(
            image="{}/node_exporter:{}".format(image_namespace,
                                               dspace_version),
            privileged=True,
            name="{}_node_exporter".format(image_namespace),
            volumes=[(config_dir, config_dir_container),
                     ("/", "/host", "ro,rslave")],
            envs=[("NODE_EXPORTER_ADDRESS", str(node.ip_address)),
                  ("NODE_EXPORTER_PORT", node_exporter_port)]
        )
        agent_port = objects.sysconfig.sys_config_get(
            ctxt, "agent_port")
        endpoint = {"ip": str(node.ip_address), "port": agent_port}
        rpc_service = objects.RPCService(
            ctxt, service_name='agent',
            hostname=node.hostname,
            endpoint=endpoint,
            cluster_id=node.cluster_id,
            node_id=node.id)
        rpc_service.create()
        self.wait_agent_ready()

    def wait_agent_ready(self, ctxt, node):
        logger.debug("wait agent ready to work")
        retry_times = 0
        while True:
            if retry_times == 30:
                logger.error("dsa cann't connect in 30 seconds")
                raise exc.InvalidInput("dsa connect error")
            try:
                agent = AgentClientManager(
                    self.ctxt, cluster_id=self.ctxt.cluster_id
                ).get_client(node.id)
                agent.check_dsa_status(ctxt)
                break
            except _Rendezvous:
                logger.debug("dsa not ready, will try connect after 1 second")
            retry_times += 1
            time.sleep(1)
        logger.info("dsa is ready")
