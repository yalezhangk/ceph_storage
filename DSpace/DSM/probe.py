from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.taskflows.probe import ProbeTask

logger = logging.getLogger(__name__)


class ProbeHandler(AdminBaseHandler):
    def probe_cluster_nodes(self, ctxt, ip, password, user="root", port=22):
        node = objects.Node(ip_address=ip, password=password)
        node_task = ProbeTask(ctxt, node)
        return node_task.probe_cluster_nodes()
