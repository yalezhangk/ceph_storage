from oslo_log import log as logging

from t2stor import objects
from t2stor.admin.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class NetworkHandler(AdminBaseHandler):
    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
        networks = objects.NetworkList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs
        )
        return networks
