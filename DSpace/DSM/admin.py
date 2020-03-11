from oslo_config import cfg
from oslo_log import log as logging

from DSpace.DSM.action_log import ActionLogHandler
from DSpace.DSM.alert_group import AlertGroupHandler
from DSpace.DSM.alert_log import AlertLogHandler
from DSpace.DSM.alert_rule import AlertRuleHandler
from DSpace.DSM.ceph_config import CephConfigHandler
from DSpace.DSM.cluster import ClusterHandler
from DSpace.DSM.component import ComponentHandler
from DSpace.DSM.cron import CronHandler
from DSpace.DSM.crush_rule import CrushRuleHandler
from DSpace.DSM.datacenter import DatacenterHandler
from DSpace.DSM.disk import DiskHandler
from DSpace.DSM.email_group import EmailGroupHandler
from DSpace.DSM.license import LicenseHandler
from DSpace.DSM.log_file import LogFileHandler
from DSpace.DSM.mail import MailHandler
from DSpace.DSM.metrics import MetricsHandler
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
from DSpace.DSM.volume import VolumeHandler
from DSpace.DSM.volume_access_path import VolumeAccessPathHandler
from DSpace.DSM.volume_client_group import VolumeClientGroupHandler
from DSpace.DSM.volume_snapshot import VolumeSnapshotHandler
from DSpace.service import ServiceCell

CONF = cfg.CONF

logger = logging.getLogger(__name__)


class AdminHandler(ActionLogHandler,
                   AlertGroupHandler,
                   AlertLogHandler,
                   AlertRuleHandler,
                   CephConfigHandler,
                   ClusterHandler,
                   ComponentHandler,
                   CronHandler,
                   CrushRuleHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   LicenseHandler,
                   LogFileHandler,
                   MailHandler,
                   MetricsHandler,
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
                   TaskHandler,
                   VolumeAccessPathHandler,
                   VolumeHandler,
                   VolumeClientGroupHandler,
                   VolumeSnapshotHandler):
    def __init__(self, *args, **kwargs):
        super(AdminHandler, self).__init__(*args, **kwargs)
        self.to_active()


class AdminService(ServiceCell):
    service_name = "admin"

    def __init__(self, *args, **kwargs):
        self.handler = AdminHandler()
        super(AdminService, self).__init__(*args, **kwargs)
