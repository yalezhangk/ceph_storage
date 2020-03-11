import random

from oslo_log import log as logging

from DSpace.DSM.base import AdminBaseHandler
from DSpace.utils.metrics import Metric

logger = logging.getLogger(__name__)


class MetricsHandler(AdminBaseHandler):
    metrics_lock = None
    metrics = None

    def __init__(self, *args, **kwargs):
        self._setup_metrics()

    def _setup_metrics(self):
        self.metrics = {}
        self.metrics['example'] = Metric(
            'gauge',
            'example',
            'This is a example',
            ('cluster_id', 'rgw')
        )

    def _get_metrics(self):
        r = random.choice([10, 20, 50, 70, 99])
        self.metrics['example'].set(r, ("aaa", "aaa1"))
        self.metrics['example'].set(r, ("aaa", "aaa2"))
        self.metrics['example'].set(r, ("bbb", "bbb1"))
        self.metrics['example'].set(r, ("bbb", "bbb2"))

    def metrics_content(self, ctxt):
        self._get_metrics()
        if not self.metrics:
            return ""
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        return ''.join(_metrics) + '\n'
