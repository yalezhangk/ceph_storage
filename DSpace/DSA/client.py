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

    def ceph_osd_package_install(self, ctxt):
        response = self.call(ctxt, "ceph_osd_package_install")
        return response

    def ceph_osd_package_uninstall(self, ctxt):
        response = self.call(ctxt, "ceph_osd_package_uninstall")
        return response

    def ceph_package_uninstall(self, ctxt):
        response = self.call(ctxt, "ceph_package_uninstall")
        return response

    def check_dsa_status(self, ctxt):
        response = self.call(ctxt, "check_dsa_status")
        return response

    def ceph_osd_create(self, ctxt, osd):
        response = self.call(ctxt, "ceph_osd_create", osd=osd)
        return response

    def ceph_mon_create(self, ctxt, fsid, ceph_auth='none'):
        response = self.call(
            ctxt, "ceph_mon_create", fsid=fsid, ceph_auth=ceph_auth)
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

    def read_log_file_content(self, ctxt, node, directory, filename):
        response = self.call(ctxt, 'read_log_file_content', node=node,
                             directory=directory, filename=filename)
        return response

    def mount_bgw(self, ctxt, access_path, node):
        response = self.call(ctxt, "mount_bgw", access_path=access_path,
                             node=node)
        return response

    def unmount_bgw(self, ctxt, access_path):
        response = self.call(ctxt, "unmount_bgw", access_path=access_path)
        return response

    def bgw_set_chap(self, ctxt, node, access_path, chap_enable,
                     username, password):
        response = self.call(ctxt, "bgw_set_chap", node=node,
                             access_path=access_path, chap_enable=chap_enable,
                             username=username, password=password)
        return response

    def bgw_create_mapping(self, ctxt, node, access_path,
                           volume_client, volumes):
        response = self.call(ctxt, "bgw_create_mapping",
                             node=node, access_path=access_path,
                             volume_client=volume_client, volumes=volumes)
        return response

    def bgw_remove_mapping(self, ctxt, node, access_path,
                           volume_client, volumes):
        response = self.call(ctxt, "bgw_remove_mapping",
                             node=node, access_path=access_path,
                             volume_client=volume_client, volumes=volumes)
        return response

    def bgw_add_volume(self, ctxt, node, access_path, volume_client, volumes):
        response = self.call(ctxt, "bgw_add_volume",
                             node=node, access_path=access_path,
                             volume_client=volume_client, volumes=volumes)
        return response

    def bgw_remove_volume(self, ctxt, node, access_path, volume_client,
                          volumes):
        response = self.call(ctxt, "bgw_remove_volume",
                             node=node, access_path=access_path,
                             volume_client=volume_client, volumes=volumes)
        return response

    def bgw_change_client_group(self, ctxt, access_path, volumes,
                                volume_clients, new_volume_clients):
        response = self.call(ctxt, "bgw_change_client_group",
                             access_path=access_path,
                             volumes=volumes,
                             volume_clients=volume_clients,
                             new_volume_clients=new_volume_clients)
        return response

    def bgw_set_mutual_chap(self, ctxt, access_path, volume_clients,
                            mutual_chap_enable, mutual_username,
                            mutual_password):
        response = self.call(ctxt, "bgw_set_mutual_chap",
                             access_path=access_path,
                             volume_clients=volume_clients,
                             mutual_chap_enable=mutual_chap_enable,
                             mutual_username=mutual_username,
                             mutual_password=mutual_password)
        return response

    def prometheus_target_add(self, ctxt, ip, port, hostname, path):
        response = self.call(ctxt, 'prometheus_target_add',
                             ip=ip, port=port, hostname=hostname, path=path)
        return response

    def prometheus_target_add_all(self, ctxt, new_targets, path):
        response = self.call(ctxt, 'prometheus_target_add_all',
                             new_targets=new_targets, path=path)
        return response

    def prometheus_target_remove(self, ctxt, ip, port, hostname, path):
        response = self.call(ctxt, 'prometheus_target_remove',
                             ip=ip, port=port, hostname=hostname, path=path)
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
