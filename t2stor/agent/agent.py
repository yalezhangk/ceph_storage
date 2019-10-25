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
from t2stor.service import ServiceBase
from t2stor.tools.base import Executor
from t2stor.tools.base import SSHExecutor
from t2stor.tools.docker import Docker as DockerTool
from t2stor.tools.service import Service

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

    def __init__(self):
        self.executors = futures.ThreadPoolExecutor(max_workers=10)
        self.executors.submit(self._cron())

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

    def ceph_conf_write(self, context, conf):
        logger.debug("Write Ceph Conf")
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
        tool = Service(self.executor)
        tool.restart(name)
        return True

    def service_check(self):
        logger.debug("Get services status")
        context = RequestContext(user_id="xxx", project_id="stor",
                                 is_admin=False, cluster_id=CONF.cluster_id)
        client = AdminClientManager(
            context, CONF.cluster_id, async_support=False
        ).get_client()

        node = client.node_get(context, node_id=CONF.node_id)

        try:
            ssh_client = SSHExecutor(hostname=node.hostname,
                                     password=node.password)
        except exception.StorException as e:
            logger.error("Connect to {} failed: {}".format(CONF.my_ip, e))
            return

        docker_tool = DockerTool(ssh_client)
        service_tool = Service(ssh_client)
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
        response = client.service_update(context, json.dumps(services))
        if not response:
            logger.debug('Update service status failed!')
            return False
        logger.debug('Update service status success!')
        return True

    def disk_smart_get(self, ctxt, name):
        fake_data = [
            {
                "ATTRIBUTE_NAME": "Raw_Read_Error_Rate",
                "VALUE": "078",
                "WORST": "064",
                "THRESH": "044",
                "TYPE": "Pre-fail",
                "UPDATED": "Always",
                "RAW_VALUE": "67028116"
            },
            {
                "ATTRIBUTE_NAME": "Spin_Up_Time",
                "VALUE": "092",
                "WORST": "092",
                "THRESH": "000",
                "TYPE": "Pre-fail",
                "UPDATED": "Always",
                "RAW_VALUE": "0"
            },
            {
                "ATTRIBUTE_NAME": "Start_Stop_Count",
                "VALUE": "100",
                "WORST": "100",
                "THRESH": "020",
                "TYPE": "Old_age",
                "UPDATED": "Always",
                "RAW_VALUE": "97"
            },
        ]
        return fake_data


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
