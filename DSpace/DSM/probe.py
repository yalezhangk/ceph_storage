from oslo_log import log as logging

from DSpace.DSM.base import AdminBaseHandler
from DSpace.tools.probe import probe_cluster_nodes

logger = logging.getLogger(__name__)


class ProbeHandler(AdminBaseHandler):
    def probe_cluster_nodes(self, ctxt, ip, password, user="root", port=22):
        return probe_cluster_nodes(ip=ip, password=password, port=port,
                                   user=user)
