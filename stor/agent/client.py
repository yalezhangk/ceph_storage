from __future__ import print_function
import sys

from oslo_log import log as logging

from stor import objects
from stor.common.config import CONF
from stor.service import BaseClient
from stor.service import BaseClientManager
from stor.context import RequestContext
from stor import version


logger = logging.getLogger(__name__)


class AgentClient(BaseClient):
    service_name = "agent"

    def disk_get_all(self, ctxt):
        response = self.call(ctxt, "disk_get_all")
        return response

    def ceph_conf_write(self, ctxt, ceph_conf):
        response = self.call(ctxt, "disk_get_all", ceph_conf=ceph_conf)
        return response

    def package_install(self, ctxt, packages):
        response = self.call(ctxt, "package_install", packages=packages)
        return response


class AgentClientManager(BaseClientManager):
    service_name = "agent"
    client_cls = AgentClient


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client = AgentClientManager(
        ctxt, cluster_id='7be530ce').get_client("devel")
    print(client.disk_get_all(ctxt))
