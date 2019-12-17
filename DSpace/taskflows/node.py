import logging
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
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import ConfigKey
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
from DSpace.utils.cluster_config import get_full_ceph_version

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
                ctxt, filters={"cluster_id": "*", "ip_address": ip_address}):
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

    def __init__(self, ctxt, node):
        self.ctxt = ctxt
        self.node = node

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

    def get_chrony_conf(self, ctxt, ip):
        chrony_server = objects.sysconfig.sys_config_get(ctxt,
                                                         "chrony_server")
        tpl = template.get('chrony.conf.j2')
        chrony_conf = tpl.render(chrony_server=chrony_server,
                                 ip_address=str(ip))
        return chrony_conf

    def chrony_update(self):
        ssh = self.get_ssh_executor()
        # install package
        config_dir = objects.sysconfig.sys_config_get(self.ctxt, "config_dir")
        image_namespace = objects.sysconfig.sys_config_get(self.ctxt,
                                                           "image_namespace")
        file_tool = FileTool(ssh)
        file_tool.write("{}/chrony.conf".format(config_dir),
                        self.get_chrony_conf(self.ctxt, self.node.ip_address))

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

        ceph_auth = objects.ceph_config.ceph_config_get(
            self.ctxt, 'global', 'auth_cluster_required')
        fsid = objects.ceph_config.ceph_config_get(self.ctxt, 'global', 'fsid')
        mgr_dspace_port = objects.sysconfig.sys_config_get(
            self.ctxt, "mgr_dspace_port")
        agent.ceph_mon_create(self.ctxt, fsid, ceph_auth=ceph_auth,
                              mgr_dspace_port=mgr_dspace_port)

    def ceph_config_update(self, ctxt, values):
        logger.info("update ceph config")
        agent = self.get_agent()
        res = agent.ceph_config_update(ctxt, values)
        return res

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

    def ceph_rgw_package_install(self):
        logger.info("Install radosgw package on node")
        agent = self.get_agent()
        agent.ceph_rgw_package_install(self.ctxt)

    def ceph_rgw_package_uninstall(self):
        logger.info("uninstall radosgw package on node")
        agent = self.get_agent()
        agent.ceph_rgw_package_uninstall(self.ctxt)

    def ceph_rgw_install(self, radosgw, zone_params):
        # write ceph.conf
        ceph_conf_content = objects.ceph_config.ceph_config_content(self.ctxt)
        logger.debug("Write radosgw config for %s", radosgw.name)
        agent = self.get_agent()
        agent.ceph_conf_write(self.ctxt, ceph_conf_content)

        logger.debug("Radosgw %s create on node %s",
                     radosgw.name, radosgw.node_id)
        radosgw = agent.ceph_rgw_create(self.ctxt, radosgw, zone_params)
        return radosgw

    def ceph_rgw_uninstall(self, radosgw):
        logger.debug("radosgw %s remove on node %s",
                     radosgw.name, radosgw.node_id)
        agent = self.get_agent()
        radosgw = agent.ceph_rgw_destroy(self.ctxt, radosgw)
        return radosgw

    def rgw_router_install(self, rgw_router, net_id):
        wf = lf.Flow('Install Haproxy')
        wf.add(HaproxyInstall('Install Haproxy'))
        if rgw_router.virtual_ip:
            wf.add(KeepalivedInstall('Install Keepalived'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            "rgw_router": rgw_router,
            "net_id": net_id,
            'task_info': {}
        })

    def rgw_router_uninstall(self, rgw_router):
        wf = lf.Flow('Uninstall Haproxy')
        wf.add(HaproxyUninstall('Uninstall Haproxy'))
        if rgw_router.virtual_ip:
            wf.add(KeepalivedUninstall('Uninstall Keepalived'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            "rgw_router": rgw_router,
            'task_info': {}
        })

    def rgw_router_update(self):
        wf = lf.Flow('Update Haproxy')
        wf.add(HaproxyUpdate('Update Haproxy'))
        self.node.executer = self.get_ssh_executor()
        taskflow.engines.run(wf, store={
            "ctxt": self.ctxt,
            "node": self.node,
            'task_info': {}
        })

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
        enable_ceph_repo = objects.sysconfig.sys_config_get(
            self.ctxt, ConfigKey.ENABLE_CEPH_REPO)
        if enable_ceph_repo:
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

    def pull_logfile(self, directory, filename, local_logfile_dir):
        try:
            sftp_client, transport = self.get_sftp_client()
            sftp_client.get('{}{}'.format(directory, filename),
                            '{}{}'.format(local_logfile_dir, filename))
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

    def check_container(self):
        ssh = self.get_ssh_executor()
        service_tool = ServiceTool(ssh)
        if service_tool.status('docker') == 'active':
            image_namespace = objects.sysconfig.sys_config_get(
                self.ctxt, "image_namespace")
            dspace_containers = [
                "{}_dsa".format(image_namespace),
                "{}_chrony".format(image_namespace),
                "{}_node_exporter".format(image_namespace)
            ]
            docker_tool = DockerTool(ssh)
            for container in dspace_containers:
                status = docker_tool.status(container)
                if status == 'active':
                    return False
        return True

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
        result |= sys_tool.check_package('rbd')
        result |= sys_tool.check_package('rados')
        result |= sys_tool.check_package('rgw')
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
                # default 9283
                port = objects.sysconfig.sys_config_get(
                    self.ctxt, "mgr_dspace_port")
        port = str(port)
        ip = self.node.ip_address
        hostname = self.node.hostname

        admin_node = objects.NodeList.get_all(
            self.ctxt, filters={'role_admin': 1, 'cluster_id': '*'})

        if service == 'node_exporter':
            targets = []
            for node in admin_node:
                targets.append({
                    "targets": [str(node.ip_address) + ":" + port],
                    "labels": {
                        "hostname": node.hostname,
                        "cluster_id": node.cluster_id
                    }
                })

        for node in admin_node:
            client = AgentClientManager(
                self.ctxt, cluster_id=node.cluster_id
            ).get_client(node_id=node.id)

            if action == "add":
                logger.info("Add to %s prometheus target file: %s, %s",
                            node.hostname, ip, port)
                if service == 'node_exporter':
                    targets.append({
                        "targets": [str(ip) + ":" + port],
                        "labels": {
                            "hostname": hostname,
                            "cluster_id": self.node.cluster_id
                        }
                    })
                    client.prometheus_target_add_all(
                        self.ctxt, new_targets=targets,
                        path=path + '/prometheus/targets/targets.json')
                    continue

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
        logger.info("ceph_repo: %s", ceph_repo)
        tpl = template.get('ceph.repo.j2')
        repo = tpl.render(ceph_repo=ceph_repo)
        logger.info("ceph_repo content: %s", repo)
        return repo


class InstallDocker(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(InstallDocker, self).execute(task_info)
        ssh = node.executer
        # write config
        file_tool = FileTool(ssh)
        # backup repo
        # not remove old repo
        remove_repo = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.REMOVE_ANOTHER_REPO)
        if remove_repo:
            file_tool.mkdir("/etc/yum.repos.d/bak")
            file_tool.mv("/etc/yum.repos.d/*.repo",
                         "/etc/yum.repos.d/bak/")
        # set repo
        file_tool.write("/etc/yum.repos.d/dspace.repo",
                        self.get_dspace_repo(ctxt))
        # clean repo cache
        package_tool = PackageTool(ssh)
        package_tool.clean()

        # install docker
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
        restart = True
        if debug_mode == "yes":
            volumes.append((code_dir, CODE_DIR_CONTAINER))
            restart = False
        docker_tool = DockerTool(ssh)
        docker_tool.run(
            name="{}_dsa".format(image_namespace),
            image="{}/dspace:{}".format(image_namespace, dspace_version),
            command="dsa",
            privileged=True,
            restart=restart,
            volumes=volumes
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
        self.wait_agent_ready(ctxt, node)

    def wait_agent_ready(self, ctxt, node):
        logger.debug("wait agent ready to work")
        retry_times = 0
        while True:
            if retry_times == 30:
                logger.error("dsa cann't connect in 30 seconds")
                raise exc.InvalidInput("dsa connect error")
            try:
                agent = AgentClientManager(
                    ctxt, cluster_id=ctxt.cluster_id
                ).get_client(node.id)
                state = agent.check_dsa_status(ctxt)
                if state == 'ready':
                    break
                logger.info("dsa up but not ready, will try connect"
                            "after 1 second")
            except _Rendezvous:
                logger.info("dsa not up, will try connect after 1 second")
            retry_times += 1
            time.sleep(1)
        logger.info("dsa is ready")

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


class DSpaceChronyInstall(BaseTask, NodeTask):
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


class DSpaceExpoterUninstall(BaseTask, ContainerUninstallMixin):
    def execute(self, ctxt, node, task_info):
        super(DSpaceExpoterUninstall, self).execute(task_info)

        ssh = node.executer
        self._node_remove_container(ctxt, ssh, "node_exporter",
                                    "node_exporter")


class NodeBaseTask(BaseTask):
    def _get_agent(self, ctxt, node):
        client = AgentClientManager(
            ctxt, cluster_id=ctxt.cluster_id
        ).get_client(node.id)
        return client


class OsdUninstall(NodeBaseTask):
    def execute(self, ctxt, osd, task_info):
        super(OsdUninstall, self).execute(task_info)
        logger.info("uninstall ceph osd %s on node %s", osd.id, osd.node.id)
        agent = self._get_agent(ctxt, osd.node)
        osd = agent.ceph_osd_destroy(ctxt, osd)
        osd.destroy()


class MonUninstall(NodeBaseTask):
    def execute(self, ctxt, node, task_info):
        super(MonUninstall, self).execute(task_info)
        logger.info("uninstall ceph mon on node %s", node.id)
        agent = self._get_agent(ctxt, node)
        agent.ceph_mon_remove(ctxt, last_mon=True)
        node.role_monitor = False
        node.save()


class StorageUninstall(NodeBaseTask):
    def execute(self, ctxt, node, task_info):
        super(StorageUninstall, self).execute(task_info)
        logger.info("uninstall ceph osd package on node %s", node.id)
        agent = self._get_agent(ctxt, node)
        agent.ceph_osd_package_uninstall(ctxt)
        node.role_storage = False
        node.save()


class CephPackageUninstall(NodeBaseTask):
    def execute(self, ctxt, node, task_info):
        super(CephPackageUninstall, self).execute(task_info)
        logger.info("uninstall ceph package on node %s", node.id)
        try:
            agent = self._get_agent(ctxt, node)
            agent.ceph_package_uninstall(ctxt)
        except Exception as e:
            logger.warning("uninstall ceph package failed: %s", e)


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


def get_haproxy_cfg(ctxt, node):
    haproxy_services = objects.RouterServiceList.get_all(
        ctxt, filters={'node_id': node.id, 'name': 'haproxy'})
    tpl = template.get('haproxy.cfg.j2')
    cfg_params = tpl.render()
    for service in haproxy_services:
        router = objects.RadosgwRouter.get_by_id(ctxt, service.router_id,
                                                 joined_load=True)
        tpl = template.get('haproxy_front.j2')
        vip = node.object_gateway_ip_address
        if router.virtual_ip:
            vip = router.virtual_ip
        cfg_params += tpl.render(backend_name=router.name,
                                 router_vip_address=vip,
                                 router_port=router.port,
                                 router_https_port=router.https_port,
                                 radosgws=router.radosgws)
    return cfg_params


class HaproxyInstall(BaseTask):
    def execute(self, ctxt, node, rgw_router, net_id, task_info):
        super(HaproxyInstall, self).execute(task_info)

        ssh = node.executer
        # get global config
        log_dir = objects.sysconfig.sys_config_get(ctxt, "log_dir")
        log_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "log_dir_container")
        config_dir = objects.sysconfig.sys_config_get(
            ctxt, "config_dir") + "/radosgw_haproxy/"
        config_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "config_dir_container") + "/haproxy"
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(ctxt,
                                                          "dspace_version")

        router_service = objects.RouterService(
            ctxt, name="haproxy",
            status='creating',
            node_id=node.id,
            cluster_id=ctxt.cluster_id,
            router_id=rgw_router.id,
            net_id=net_id
        )
        router_service.create()

        # write config
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/haproxy.cfg".format(config_dir),
                        get_haproxy_cfg(ctxt, node))

        # cp tls pem
        # TODO: auto generate pem
        tpl = template.get('haproxy.pem')
        pem_content = tpl.render()
        file_tool.write("{}/haproxy_{}.pem".format(
            config_dir, rgw_router.name), pem_content)

        # run container
        volumes = [
            (config_dir, config_dir_container),
            (log_dir, log_dir_container),
            ("/etc/localtime", "/etc/localtime", "ro"),
            ("/etc/pki/", "/etc/pki"),
            ("radosgw_haproxy_socket", "/var/lib/dspace/haproxy/")
        ]
        restart = True
        docker_tool = DockerTool(ssh)
        container_name = "{}_radosgw_haproxy".format(image_namespace)
        # Check container, create or restart
        if docker_tool.exist(container_name):
            docker_tool.restart(container_name)
        else:
            docker_tool.run(
                name=container_name,
                image="{}/haproxy:{}".format(image_namespace, dspace_version),
                command="haproxy",
                privileged=True,
                restart=restart,
                volumes=volumes
            )

        if docker_tool.status(container_name) == s_fields.ServiceStatus.ACTIVE:
            router_service.status = s_fields.RouterServiceStatus.ACTIVE
        else:
            logger.error("Start container %s failed", container_name)
            router_service.status = s_fields.RouterServiceStatus.ERROR
        router_service.save()


def haproxy_update(ctxt, node):
    ssh = node.executer
    # get global config
    config_dir = objects.sysconfig.sys_config_get(
        ctxt, "config_dir") + "/radosgw_haproxy/"
    image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                       "image_namespace")

    docker_tool = DockerTool(ssh)
    file_tool = FileTool(ssh)
    container_name = "{}_radosgw_haproxy".format(image_namespace)
    router_service = objects.RouterServiceList.get_all(
        ctxt, filters={'node_id': node.id, 'name': 'haproxy'}
    )
    if not router_service:
        docker_tool.rm(container_name, force=True)
        file_tool.rm("{}/haproxy.cfg".format(config_dir))
    else:
        # Update config
        file_tool.mkdir(config_dir)
        file_tool.write("{}/haproxy.cfg".format(config_dir),
                        get_haproxy_cfg(ctxt, node))
        docker_tool.restart(container_name)


class HaproxyUpdate(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(HaproxyUpdate, self).execute(task_info)
        haproxy_update(ctxt, node)


class HaproxyUninstall(BaseTask):
    def execute(self, ctxt, node, rgw_router, task_info):
        super(HaproxyUninstall, self).execute(task_info)
        haproxy_update(ctxt=ctxt, node=node)


def get_keepalived_cfg(ctxt, node):
    haproxy_services = objects.RouterServiceList.get_all(
        ctxt, filters={'node_id': node.id, 'name': 'keepalived'})
    tpl = template.get('keepalived.conf.j2')
    conf_params = tpl.render()
    for service in haproxy_services:
        tpl = template.get('keepalived_instance.j2')
        net = objects.Network.get_by_id(ctxt, service.net_id)
        router = objects.RadosgwRouter.get_by_id(ctxt, service.router_id,
                                                 joined_load=True)
        conf_params += tpl.render(
            keepalived_virtual_router_id=router.virtual_router_id,
            interface_name=net.name,
            priority=len(router.radosgws) + 1,
            router_vip_address=router.virtual_ip
        )
    return conf_params


class KeepalivedInstall(BaseTask):
    def execute(self, ctxt, node, rgw_router, net_id, task_info):
        super(KeepalivedInstall, self).execute(task_info)

        ssh = node.executer
        # get global config
        log_dir = objects.sysconfig.sys_config_get(ctxt, "log_dir")
        log_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "log_dir_container")
        config_dir = objects.sysconfig.sys_config_get(
            ctxt, "config_dir") + "/radosgw_keepalived/"
        config_dir_container = objects.sysconfig.sys_config_get(
            ctxt, "config_dir_container") + "/keepalived"
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        dspace_version = objects.sysconfig.sys_config_get(ctxt,
                                                          "dspace_version")
        router_service = objects.RouterService(
            ctxt, name="keepalived",
            status='creating',
            node_id=node.id,
            cluster_id=ctxt.cluster_id,
            router_id=rgw_router.id,
            net_id=net_id
        )

        router_service.create()
        # write config
        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/keepalived.conf".format(config_dir),
                        get_keepalived_cfg(ctxt, node))
        # run container
        volumes = [
            (config_dir, config_dir_container),
            (log_dir, log_dir_container),
            ("/etc/localtime", "/etc/localtime", "ro"),
            ("/lib/modules", "/lib/modules", "ro"),
            ("radosgw_haproxy_socket", "/var/lib/dspace/haproxy/"),
            ("radosgw_ha_state", "/var/lib/ha_state/"),
        ]
        restart = True
        docker_tool = DockerTool(ssh)
        container_name = "{}_radosgw_keepalived".format(image_namespace)
        if docker_tool.exist(container_name):
            docker_tool.restart(container_name)
        else:
            docker_tool.run(
                name=container_name,
                image="{}/keepalived:{}".format(
                    image_namespace, dspace_version),
                command="keepalived",
                privileged=True,
                restart=restart,
                volumes=volumes
            )

        if docker_tool.status(container_name) == s_fields.ServiceStatus.ACTIVE:
            router_service.status = s_fields.RouterServiceStatus.ACTIVE
        else:
            logger.error("Start container %s failed", container_name)
            router_service.status = s_fields.RouterServiceStatus.ERROR
        router_service.save()


class KeepalivedUninstall(BaseTask):
    def execute(self, ctxt, node, rgw_router, task_info):
        super(KeepalivedUninstall, self).execute(task_info)

        ssh = node.executer
        # get global config
        config_dir = objects.sysconfig.sys_config_get(
            ctxt, "config_dir") + "/radosgw_keepalived/"
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        container_name = "{}_radosgw_keepalived".format(image_namespace)

        file_tool = FileTool(ssh)
        file_tool.mkdir(config_dir)
        file_tool.write("{}/keepalived.conf".format(config_dir),
                        get_keepalived_cfg(ctxt, node))

        docker_tool = DockerTool(ssh)
        docker_tool.restart(container_name)

        router_service = objects.RouterServiceList.get_all(
            ctxt, filters={'node_id': node.id, 'name': 'keepalived'}
        )
        if not router_service:
            docker_tool.rm(container_name, force=True)
            file_tool.rm("{}/keepalived.conf".format(config_dir))


class SyncCephVersion(BaseTask):
    def execute(self, ctxt, node, task_info):
        super(SyncCephVersion, self).execute(task_info)
        ceph_version = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.CEPH_VERSION)
        if not ceph_version:
            probe_tool = ProbeTool(node.executer)
            checks = probe_tool.check(['ceph_version'])
            ceph_version = checks['ceph_version']
            installed = ceph_version.get('installed')
            version = get_full_ceph_version(installed)
            objects.sysconfig.sys_config_set(
                ctxt, ConfigKey.CEPH_VERSION, version)
