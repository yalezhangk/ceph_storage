from __future__ import print_function

import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClient
from DSpace.service import BaseClientManager

logger = logging.getLogger(__name__)


class AgentClient(BaseClient):
    service_name = "agent"

    def disk_get_all(self, ctxt):
        response = self.call(ctxt, "disk_get_all")
        return response

    def ceph_conf_write(self, ctxt, content):
        response = self.call(ctxt, "ceph_conf_write", content=content)
        return response

    def ceph_osd_create(self, ctxt, osd):
        response = self.call(ctxt, "ceph_osd_create", osd=osd)
        return response

    def ceph_mon_create(self, ctxt, ceph_auth='none'):
        response = self.call(ctxt, "ceph_mon_create", ceph_auth=ceph_auth)
        return response

    def ceph_mon_remove(self, ctxt, last_mon=False):
        response = self.call(ctxt, "ceph_mon_remove", last_mon=last_mon)
        return response

    def ceph_osd_destroy(self, ctxt, osd):
        response = self.call(ctxt, "ceph_osd_destroy", osd=osd)
        return response

    def package_install(self, ctxt, packages):
        response = self.call(ctxt, "package_install", packages=packages)
        return response

    def service_restart(self, ctxt, name):
        response = self.call(ctxt, "service_restart", name=name)
        return response

    def disk_smart_get(self, ctxt, node, name):
        response = self.call(ctxt, "disk_smart_get", node=node, name=name)
        return response

    def disk_light(self, ctxt, led, node, name):
        response = self.call(ctxt, "disk_light", led=led, node=node, name=name)
        return response

    def disk_partitions_create(self, ctxt, node, disk, values):
        response = self.call(
            ctxt, "disk_partitions_create", node=node, disk=disk, values=values
        )
        return response

    def disk_partitions_remove(self, ctxt, node, name):
        response = self.call(ctxt, "disk_partitions_remove", node=node,
                             name=name)
        return response

    def ceph_config_update(self, ctxt, values):
        response = self.call(ctxt, "ceph_config_update", values=values)
        return response

    def get_logfile_metadata(self, ctxt, node, service_type):
        response = self.call(ctxt, "get_logfile_metadata", node=node,
                             service_type=service_type)
        return response

    def pull_logfile(self, ctxt, node, directory, filename):
        response = self.call(ctxt, "pull_logfile", node=node,
                             directory=directory, filename=filename)
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
    # print(client.disk_get_all(ctxt))
    print(client.service_restart(ctxt, "chronyd"))
