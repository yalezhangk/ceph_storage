import time

from oslo_log import log as logging

from t2stor.admin.action_log import ActionLogHandler
from t2stor.admin.alert_group import AlertGroupHandler
from t2stor.admin.alert_log import AlertLogHandler
from t2stor.admin.alert_rule import AlertRuleHandler
from t2stor.admin.ceph_config import CephConfigHandler
from t2stor.admin.cluster import ClusterHandler
from t2stor.admin.crush_rule import CephCrushHandler
from t2stor.admin.datacenter import DatacenterHandler
from t2stor.admin.disk import DiskHandler
from t2stor.admin.email_group import EmailGroupHandler
from t2stor.admin.mail import MailHandler
from t2stor.admin.network import NetworkHandler
from t2stor.admin.node import NodeHandler
from t2stor.admin.osd import OsdHandler
from t2stor.admin.pool import PoolHandler
from t2stor.admin.prometheus import PrometheusHandler
from t2stor.admin.rack import RackHandler
from t2stor.admin.service import ServiceHandler
from t2stor.admin.sys_config import SysConfigHandler
from t2stor.admin.volume import VolumeHandler
from t2stor.admin.volume_access_path import VolumeAccessPathHandler
from t2stor.admin.volume_client import VolumeClientHandler
from t2stor.admin.volume_snapshot import VolumeSnapshotHandler
from t2stor.service import ServiceBase

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AdminHandler(ActionLogHandler,
                   AlertGroupHandler,
                   AlertLogHandler,
                   AlertRuleHandler,
                   CephConfigHandler,
                   CephCrushHandler,
                   ClusterHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   OsdHandler,
                   MailHandler,
                   NetworkHandler,
                   NodeHandler,
                   PoolHandler,
                   PrometheusHandler,
                   RackHandler,
                   ServiceHandler,
                   SysConfigHandler,
                   VolumeAccessPathHandler,
                   VolumeHandler,
                   VolumeClientHandler,
                   VolumeSnapshotHandler):
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
