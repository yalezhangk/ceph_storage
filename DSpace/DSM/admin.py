import time

from oslo_config import cfg
from oslo_log import log as logging

from DSpace.DSM.action_log import ActionLogHandler
from DSpace.DSM.alert_group import AlertGroupHandler
from DSpace.DSM.alert_log import AlertLogHandler
from DSpace.DSM.alert_rule import AlertRuleHandler
from DSpace.DSM.ceph_config import CephConfigHandler
from DSpace.DSM.cluster import ClusterHandler
from DSpace.DSM.component import ComponentHandler
from DSpace.DSM.crush_rule import CrushRuleHandler
from DSpace.DSM.datacenter import DatacenterHandler
from DSpace.DSM.disk import DiskHandler
from DSpace.DSM.email_group import EmailGroupHandler
from DSpace.DSM.log_file import LogFileHandler
from DSpace.DSM.mail import MailHandler
from DSpace.DSM.network import NetworkHandler
from DSpace.DSM.node import NodeHandler
from DSpace.DSM.osd import OsdHandler
from DSpace.DSM.pool import PoolHandler
from DSpace.DSM.probe import ProbeHandler
from DSpace.DSM.prometheus import PrometheusHandler
from DSpace.DSM.rack import RackHandler
from DSpace.DSM.radosgw import RadosgwHandler
from DSpace.DSM.radosgw_router import RadosgwRouterHandler
from DSpace.DSM.service import ServiceHandler
from DSpace.DSM.sys_config import SysConfigHandler
from DSpace.DSM.task import TaskHandler
from DSpace.service import ServiceBase

CONF = cfg.CONF

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AdminHandler(ActionLogHandler,
                   AlertGroupHandler,
                   AlertLogHandler,
                   AlertRuleHandler,
                   CephConfigHandler,
                   ClusterHandler,
                   ComponentHandler,
                   CrushRuleHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   LogFileHandler,
                   MailHandler,
                   NetworkHandler,
                   NodeHandler,
                   OsdHandler,
                   PoolHandler,
                   ProbeHandler,
                   PrometheusHandler,
                   RadosgwHandler,
                   RadosgwRouterHandler,
                   RackHandler,
                   ServiceHandler,
                   SysConfigHandler,
                   TaskHandler):
    pass


class AdminService(ServiceBase):
    service_name = "admin"

    def __init__(self):
        self.handler = AdminHandler()
        super(AdminService, self).__init__()


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


def service():
    admin = AdminService()
    admin.start()
    run_loop()
    admin.stop()
