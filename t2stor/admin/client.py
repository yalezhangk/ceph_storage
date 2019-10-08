from __future__ import print_function
import sys

from oslo_log import log as logging

from t2stor import objects
from t2stor.common.config import CONF
from t2stor.service import BaseClient
from t2stor.service import BaseClientManager
from t2stor.context import RequestContext
from t2stor import version


class AdminClient(BaseClient):

    def get_ceph_conf(self, ctxt, ceph_host=None):
        response = self.call(ctxt, method="get_ceph_conf", ceph_host=ceph_host)
        return response

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "volume_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def volume_get(self, ctxt, volume_id):
        response = self.call(ctxt, "volume_get", volume_id=volume_id)
        return response


class AdminClientManager(BaseClientManager):
    cluster = "default"
    service_name = "admin"
    client_cls = AdminClient


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client = AdminClientManager(
        ctxt, cluster_id='7be530ce').get_client("devel")
    print(client.get_ceph_conf(ctxt))
    print(client.volume_get_all(ctxt))
    print(client.volume_get(ctxt, "a0a1ae78-d923-44aa-841e-75c4fb4fed88"))