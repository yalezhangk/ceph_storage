import json
import sys
import time
from concurrent import futures

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import version
from t2stor.admin.client import AdminClientManager
from t2stor.common.config import CONF
from t2stor.context import RequestContext
from t2stor.objects import fields as s_fields
from t2stor.service import ServiceBase
from t2stor.taskflows.node import NodeTask
from t2stor.tools.base import Executor
from t2stor.tools.base import SSHExecutor
from t2stor.tools.ceph import CephTool
from t2stor.tools.disk import DiskTool as DiskTool
from t2stor.tools.docker import Docker as DockerTool
from t2stor.tools.file import File as FileTool
from t2stor.tools.log_file import LogFile as LogFileTool
from t2stor.tools.package import Package as PackageTool
from t2stor.tools.pysmart import Device as DevideTool
from t2stor.tools.service import Service as ServiceTool
from t2stor.tools.storcli import StorCli as StorCliTool

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


example = {
    "disks": [{
        "name": "vda",
        "size": 3 * 1024**3,
    }, {
        "name": "vdb",
        "size": 5 * 1024**3,
    }]
}


container_roles = ["base", "role_admin"]
service_map = {
    "base": {
        "NODE_EXPORTER": "t2stor_node_exporter",
        "CHRONY": "t2stor_chrony",
    },
    "role_monitor": {
        "MON": "ceph-mon@$HOSTNAME",
        "MGR": "ceph-mgr@$HOSTNAME",
    },
    "role_admin": {
        "PROMETHEUS": "t2stor_prometheus",
        "CONFD": "t2stor_confd",
        "ETCD": "t2stor_etcd",
        "PORTAL": "t2stor_portal",
        "NGINX": "t2stor_portal_nginx",
        "ADMIN": "t2stor_admin",
        "API": "t2stor_api",
    },
    "role_storage": {},
    "role_block_gateway": {
        "TCMU": "tcmu",
    },
    "role_object_gateway": {
        "RGW": "ceph-radosgw@rgw.$HOSTNAME",
    },
    "role_file_gateway": {},
}


class AgentHandler(object):
    Node = None
    ctxt = None

    def __init__(self):
        self.executors = futures.ThreadPoolExecutor(max_workers=10)
        self.ctxt = RequestContext(user_id="xxx", project_id="stor",
                                   is_admin=False, cluster_id=CONF.cluster_id)
        self._get_node()
        self.executors.submit(self._cron)

    def _get_node(self):
        client = AdminClientManager(
            self.ctxt, CONF.cluster_id, async_support=False
        ).get_client()
        self.node = client.node_get(self.ctxt, node_id=CONF.node_id)

    def _cron(self):
        logger.debug("Start crontab")
        crons = [
            self.service_check
        ]
        while True:
            for fun in crons:
                fun()
            time.sleep(60)

    def _get_executor(self):
        return Executor()

    @property
    def executor(self):
        return self._get_executor()

    def disk_get_all(self, context):
        logger.debug("disk get all")
        return []

    def ceph_conf_write(self, context, content):
        logger.debug("Write Ceph Conf")
        client = self._get_ssh_client()
        file_tool = FileTool(client)
        file_tool.mkdir("/etc/ceph")
        file_tool.write("/etc/ceph/ceph.conf", content)
        return True

    def ceph_prepare_disk(self, context, osd):
        kwargs = {
            "diskname": osd.disk.name,
            "backend": osd.type,
        }
        if osd.fsid and osd.osd_id:
            kwargs['fsid'] = osd.fsid
            kwargs['osd_id'] = osd.osd_id
        if osd.cache_partition_id:
            kwargs['cache_partition'] = osd.cache_partition.name
        if osd.db_partition_id:
            kwargs['db_partition'] = osd.db_partition.name
        if osd.wal_partition_id:
            kwargs['wal_partition'] = osd.wal_partition.name
        if osd.journal_partition_id:
            kwargs['journal_partition'] = osd.journal_partition.name

        client = self._get_ssh_client()
        ceph_tool = CephTool(client)
        ceph_tool.disk_prepare(**kwargs)
        return True

    def ceph_active_disk(self, context, osd):
        client = self._get_ssh_client()
        ceph_tool = CephTool(client)
        ceph_tool.disk_active(osd.disk.name)

    def ceph_osd_create(self, context, osd):
        self.ceph_prepare_disk(context, osd)
        self.ceph_active_disk(context, osd)
        return osd

    def ceph_osd_destroy(self, context, osd):
        client = self._get_ssh_client()
        ceph_tool = CephTool(client)
        ceph_tool.osd_deactivate(osd.disk.name)
        ceph_tool.osd_zap(osd.disk.name)
        disk_tool = DiskTool(client)
        if osd.cache_partition_id:
            disk_tool.data_clear(osd.cache_partition.name)
        if osd.db_partition_id:
            disk_tool.data_clear(osd.db_partition.name)
        if osd.wal_partition_id:
            disk_tool.data_clear(osd.wal_partition.name)
        if osd.journal_partition_id:
            disk_tool.data_clear(osd.journal_partition.name)
        return osd

    def ceph_mon_create(self, context, ceph_auth='none'):
        client = self._get_ssh_client()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-mon", "ceph-mgr"])
        # create mon dir
        file_tool = FileTool(client)
        file_tool.mkdir("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mon", user='ceph', group='ceph')
        # create mgr dir
        file_tool.mkdir("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mgr", user='ceph', group='ceph')

        ceph_tool = CephTool(client)
        ceph_tool.mon_install(self.node.hostname,
                              self.node.cluster_id,
                              ceph_auth=ceph_auth)

        # enable and start ceph-mon
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-mon@{}".format(self.node.hostname))
        service_tool.start("ceph-mon@{}".format(self.node.hostname))
        service_tool.enable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.start("ceph-mgr@{}".format(self.node.hostname))
        return True

    def package_install(self, context, packages):
        logger.debug("Install Package")
        return True

    def service_start(self, context, service_info):
        logger.debug("Install Package")
        time.sleep(1)
        logger.debug("Start Service")
        time.sleep(1)
        return True

    def service_restart(self, context, name):
        logger.debug("Service restart: %s", name)
        tool = ServiceTool(self.executor)
        tool.restart(name)
        return True

    def _get_ssh_client(self, node=None):
        if not node:
            node = self.node
        try:
            ssh_client = SSHExecutor(hostname=node.hostname,
                                     password=node.password)
        except exception.StorException as e:
            logger.error("Connect to {} failed: {}".format(CONF.my_ip, e))
            return None
        return ssh_client

    def service_check(self):
        logger.debug("Get services status")

        node = self.node
        ssh_client = self._get_ssh_client()
        if not ssh_client:
            return False
        docker_tool = DockerTool(ssh_client)
        service_tool = ServiceTool(ssh_client)
        services = []

        for role, sers in six.iteritems(service_map):
            if (role != "base") and (not node[role]):
                continue
            for k, v in six.iteritems(sers):
                v = v.replace('$HOSTNAME', node.hostname)
                try:
                    if role in container_roles:
                        status = docker_tool.status(name=v)
                    else:
                        status = service_tool.status(name=v)
                except exception.StorException as e:
                    logger.error("Get service status error: {}".format(e))
                    status = 'inactive'
                services.append({
                    "name": k,
                    "status": status,
                    "node_id": CONF.node_id
                })
        logger.debug(services)
        response = self.node.service_update(self.ctxt, json.dumps(services))
        if not response:
            logger.debug('Update service status failed!')
            return False
        logger.debug('Update service status success!')
        return True

    def disk_smart_get(self, ctxt, node, name):
        ssh_client = self._get_ssh_client(node)
        if not ssh_client:
            return []

        disk_name = '/dev/' + name
        device_tool = DevideTool(name=disk_name, ssh=ssh_client)
        smart = device_tool.all_attributes()
        if not smart:
            return [
                {
                    "raw": "0",
                    "updated": "Always",
                    "num": "5",
                    "worst": "100",
                    "name": "Reallocated_Sector_Ct",
                    "when_failed": "-",
                    "thresh": "000",
                    "type": "Old_age",
                    "value": "100"
                },
                {
                    "raw": "9828",
                    "updated": "Always",
                    "num": "9",
                    "worst": "100",
                    "name": "Power_On_Hours",
                    "when_failed": "-",
                    "thresh": "000",
                    "type": "Old_age",
                    "value": "100"
                }
            ]
        return smart

    def disk_light(self, ctxt, led, node, name):
        logger.debug("Disk Light: %s", name)
        ssh_client = self._get_ssh_client(node)
        if not ssh_client:
            return False

        disk_name = '/dev/' + name
        storcli = StorCliTool(ssh=ssh_client, disk_name=disk_name)
        action = 'start' if led == 'on' else 'stop'
        return storcli.disk_light(action)

    def _get_disk_partition_steps(self, num, role, name, disk_size):
        step = int(100 / num)
        i = 1
        now = 0
        steps = ["0%"]
        partitions = []
        if role == s_fields.DiskPartitionRole.MIX:
            db = int(step / 5)
            db_size = disk_size * db / 100
            cache_size = disk_size * (step - db) / 100
            while i <= num:
                steps.append(str(now + db) + "%")
                partitions.append({
                    "name": name + str(i * 2 - 1),
                    "size": db_size,
                    "role": "db",
                })
                steps.append(str(now + step) + "%")
                partitions.append({
                    "name": name + str(i * 2),
                    "size": cache_size,
                    "role": "cache",
                })
                now += step
                i += 1
        else:
            partition_size = disk_size * step / 100
            while i <= num:
                now += step
                steps.append(str(now) + "%")
                partitions.append({
                    "name": name + str(i),
                    "size": partition_size,
                    "role": role,
                })
                i += 1
        logger.debug("Partition steps: {}".format(steps))
        return steps, partitions

    def disk_partitions_create(self, ctxt, node, disk, values):
        logger.debug('Make cache disk partition: %s', disk.name)
        ssh_client = self._get_ssh_client(node)
        if not ssh_client:
            return []
        disk_tool = DiskTool(ssh_client)
        partition_num = values.get('partition_num')
        partition_role = values.get('partition_role')
        steps, partitions = self._get_disk_partition_steps(
            partition_num, partition_role, disk.name, disk.disk_size)
        try:
            disk_tool.partitions_clear(disk.name)
            disk_tool.partitions_create(disk.name, steps)
            logger.debug("Partitions: {}".format(partitions))
            return partitions
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            return []

    def disk_partitions_remove(self, ctxt, node, name):
        logger.debug('Remove cache disk partitions: %s', name)
        ssh_client = self._get_ssh_client(node)
        if not ssh_client:
            return False
        disk_tool = DiskTool(ssh_client)
        try:
            _success = disk_tool.partitions_clear(name)
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            _success = False
        return _success

    def osd_create(self, ctxt, node, osd):
        task = NodeTask(ctxt, node)
        task.ceph_osd_install(osd)

    def ceph_config_update(self, ctxt, values):
        logger.debug('Update ceph config for this node')
        node_task = NodeTask(ctxt, node=None)
        try:
            node_task.ceph_config_update(values)
        except exception.StorException as e:
            logger.error('Update ceph config error: {}'.format(e))
            return False
        return True

    def get_logfile_metadata(self, ctxt, node, service_type):
        logger.debug('begin get_logfile_metadata,service_type:%s',
                     service_type)
        ssh_client = self._get_ssh_client(node)
        if not ssh_client:
            return False
        log_file_tool = LogFileTool(ssh_client)
        try:
            metadata = log_file_tool.get_logfile_metadata(service_type)
            logger.info("get_logfile_metadata success:{}".format(service_type))
        except exception.StorException as e:
            logger.error("get_logfile_metadata error:{}".format(e))
            metadata = None
        return metadata


class AgentService(ServiceBase):
    service_name = "agent"

    def __init__(self):
        self.handler = AgentHandler()
        super(AgentService, self).__init__()


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


def service():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    agent = AgentService()
    agent.start()
    run_loop()
    agent.stop()
