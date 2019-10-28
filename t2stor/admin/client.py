from __future__ import print_function

import sys

from oslo_log import log as logging

from t2stor import objects
from t2stor import version
from t2stor.common.config import CONF
from t2stor.context import RequestContext
from t2stor.service import BaseClient
from t2stor.service import BaseClientManager


class AdminClient(BaseClient):

    def get_ceph_conf(self, ctxt, ceph_host=None):
        response = self.call(ctxt, method="get_ceph_conf", ceph_host=ceph_host)
        return response

    ###################

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "volume_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def volume_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_create", data=data)
        return response

    def volume_get(self, ctxt, volume_id):
        response = self.call(ctxt, "volume_get", volume_id=volume_id)
        return response

    def volume_update(self, ctxt, volume_id, data):
        response = self.call(ctxt, "volume_update",
                             volume_id=volume_id,
                             data=data)
        return response

    def volume_delete(self, ctxt, volume_id):
        response = self.call(ctxt, "volume_delete",
                             volume_id=volume_id)
        return response

    def volume_extend(self, ctxt, volume_id, data):
        response = self.call(ctxt, "volume_extend",
                             volume_id=volume_id,
                             data=data)
        return response

    def volume_shrink(self, ctxt, volume_id, data):
        response = self.call(ctxt, "volume_shrink",
                             volume_id=volume_id,
                             data=data)
        return response

    def volume_rollback(self, ctxt, volume_id, data):
        response = self.call(ctxt, "volume_rollback",
                             volume_id=volume_id,
                             data=data)
        return response

    def volume_unlink(self, ctxt, volume_id, data):
        response = self.call(ctxt, "volume_unlink",
                             volume_id=volume_id,
                             data=data)
        return response

    ###################

    def cluster_get_info(self, ctxt, ip_address, password=None):
        response = self.call(
            ctxt, "cluster_get_info", ip_address=ip_address,
            password=password)
        return response

    def cluster_install_agent(self, ctxt, ip_address, password=None):
        response = self.call(ctxt, "cluster_install_agent",
                             ip_address=ip_address, password=password)
        return response

    ###################

    def alert_rule_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "alert_rule_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def alert_rule_get(self, ctxt, alert_rule_id):
        response = self.call(ctxt, "alert_rule_get",
                             alert_rule_id=alert_rule_id)
        return response

    def alert_rule_update(self, ctxt, alert_rule_id, data):
        response = self.call(ctxt, "alert_rule_update",
                             alert_rule_id=alert_rule_id,
                             data=data)
        return response

    def node_get(self, ctxt, node_id):
        response = self.call(ctxt, "node_get", node_id=node_id)
        return response

    def node_create(self, ctxt, data):
        response = self.call(ctxt, "node_create", data=data)
        return response

    def node_update(self, ctxt, node_id, data):
        response = self.call(ctxt, "node_update", node_id=node_id, data=data)
        return response

    def node_delete(self, ctxt, node_id):
        response = self.call(ctxt, "node_delete", node_id=node_id)
        return response

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "node_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "network_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def sysconf_get_all(self, ctxt):
        response = self.call(ctxt, "sysconf_get_all")
        return response

    def update_chrony(self, ctxt, chrony_server):
        response = self.call(ctxt, "update_chrony",
                             chrony_server=chrony_server)
        return response

    def update_sysinfo(self, ctxt, cluster_name, admin_cidr, public_cidr,
                       cluster_cidr, gateway_cidr):
        response = self.call(
            ctxt, "update_sysinfo", cluster_name=cluster_name,
            admin_cidr=admin_cidr, public_cidr=public_cidr,
            cluster_cidr=cluster_cidr, gateway_cidr=gateway_cidr)
        return response

    ###################
    def osd_get(self, ctxt, osd_id):
        response = self.call(ctxt, "osd_get", osd_id=osd_id)
        return response

    def osd_create(self, ctxt, data):
        response = self.call(ctxt, "osd_create", data=data)
        return response

    def osd_update(self, ctxt, osd_id, data):
        response = self.call(ctxt, "osd_update", osd_id=osd_id, data=data)
        return response

    def osd_delete(self, ctxt, osd_id):
        response = self.call(ctxt, "osd_delete", osd_id=osd_id)
        return response

    def osd_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "osd_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    ###################

    def datacenter_create(self, ctxt):
        response = self.call(ctxt, "datacenter_create")
        return response

    def datacenter_get(self, ctxt, datacenter_id):
        response = self.call(ctxt, "datacenter_get",
                             datacenter_id=datacenter_id)
        return response

    def datacenter_delete(self, ctxt, datacenter_id):
        response = self.call(ctxt, "datacenter_delete",
                             datacenter_id=datacenter_id)
        return response

    def datacenter_get_all(self, ctxt):
        response = self.call(ctxt, "datacenter_get_all")
        return response

    def datacenter_update(self, ctxt, datacenter_id, datacenter_name):
        response = self.call(ctxt, "datacenter_update",
                             id=datacenter_id, name=datacenter_name)
        return response

    def datacenter_racks(self, ctxt, datacenter_id):
        response = self.call(ctxt, "datacenter_racks",
                             datacenter_id=datacenter_id)
        return response

    ###################

    def rack_create(self, ctxt, datacenter_id):
        response = self.call(ctxt, "rack_create", datacenter_id=datacenter_id)
        return response

    def rack_get(self, ctxt, rack_id):
        response = self.call(ctxt, "rack_get",
                             rack_id=rack_id)
        return response

    def rack_delete(self, ctxt, rack_id):
        response = self.call(ctxt, "rack_delete",
                             rack_id=rack_id)
        return response

    def rack_get_all(self, ctxt):
        response = self.call(ctxt, "rack_get_all")
        return response

    def rack_update_name(self, ctxt, rack_id, rack_name):
        response = self.call(ctxt, "rack_update_name",
                             id=rack_id, name=rack_name)
        return response

    def rack_update_toplogy(self, ctxt, rack_id, datacenter_id):
        response = self.call(ctxt, "rack_update_toplogy",
                             id=rack_id, datacenter_id=datacenter_id)
        return response

    ###################

    def disk_get(self, ctxt, disk_id):
        response = self.call(ctxt, "disk_get", disk_id=disk_id)
        return response

    def disk_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "disk_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def disk_update(self, ctxt, disk_id, disk_type):
        response = self.call(
            ctxt, "disk_update", disk_id=disk_id, disk_type=disk_type)
        return response

    def disk_light(self, ctxt, disk_id, led):
        response = self.call(ctxt, "disk_light", disk_id=disk_id, led=led)
        return response

    def disk_cache(self, ctxt, disk_id, values):
        response = self.call(ctxt, "disk_cache", disk_id=disk_id,
                             values=values)
        return response

    def disk_smart_get(self, ctxt, disk_id):
        response = self.call(ctxt, "disk_smart_get", disk_id=disk_id)
        return response

    def disk_partition_get_all(self, ctxt, marker=None, limit=None,
                               sort_keys=None, sort_dirs=None, filters=None,
                               offset=None):
        response = self.call(ctxt, "disk_partition_get_all", marker=marker,
                             limit=limit, sort_keys=sort_keys,
                             sort_dirs=sort_dirs, filters=filters,
                             offset=offset)
        return response

    ###################

    def email_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None):
        response = self.call(
            ctxt, "email_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def email_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "email_group_create", data=data)
        return response

    def email_group_get(self, ctxt, email_group_id):
        response = self.call(ctxt, "email_group_get",
                             email_group_id=email_group_id)
        return response

    def email_group_update(self, ctxt, email_group_id, data):
        response = self.call(ctxt, "email_group_update",
                             email_group_id=email_group_id,
                             data=data)
        return response

    def email_group_delete(self, ctxt, email_group_id):
        response = self.call(ctxt, "email_group_delete",
                             email_group_id=email_group_id)
        return response

    ###################

    def alert_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None):
        response = self.call(
            ctxt, "alert_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def alert_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "alert_group_create", data=data)
        return response

    def alert_group_get(self, ctxt, alert_group_id):
        response = self.call(ctxt, "alert_group_get",
                             alert_group_id=alert_group_id)
        return response

    def alert_group_update(self, ctxt, alert_group_id, data):
        response = self.call(ctxt, "alert_group_update",
                             alert_group_id=alert_group_id,
                             data=data)
        return response

    def alert_group_delete(self, ctxt, alert_group_id):
        response = self.call(ctxt, "alert_group_delete",
                             alert_group_id=alert_group_id)
        return response

    ###################

    def services_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                         sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "services_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def service_update(self, ctxt, services):
        response = self.call(ctxt, "service_update", services=services)
        return response

    ##################

    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "pool_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def pool_get(self, ctxt, pool_id):
        response = self.call(ctxt, "pool_get", pool_id=pool_id)
        return response

    def pool_osds_get(self, ctxt, pool_id):
        response = self.call(ctxt, "pool_osds_get", pool_id=pool_id)
        return response

    ##################

    def volume_client_group_get_all(self, ctxt, marker=None, limit=None,
                                    sort_keys=None, sort_dirs=None,
                                    filters=None, offset=None):
        response = self.call(
            ctxt, "volume_client_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def volume_client_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_client_group_create", data=data)
        return response

    def volume_client_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_client_create", data=data)
        return response

    ##################

    def alert_log_get_all(self, ctxt, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None, filters=None,
                          offset=None):
        response = self.call(
            ctxt, "alert_log_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def alert_log_create(self, ctxt, data):
        response = self.call(
            ctxt, "alert_log_create", data=data)
        return response

    def alert_log_get(self, ctxt, alert_log_id):
        response = self.call(ctxt, "alert_log_get",
                             alert_log_id=alert_log_id)
        return response

    def alert_log_update(self, ctxt, alert_log_id, data):
        response = self.call(ctxt, "alert_log_update",
                             alert_log_id=alert_log_id,
                             data=data)
        return response

    def alert_log_delete(self, ctxt, alert_log_id):
        response = self.call(ctxt, "alert_log_delete",
                             alert_log_id=alert_log_id)
        return response

    ###################

    def log_file_get_all(self, ctxt, marker=None, limit=None,
                         sort_keys=None, sort_dirs=None, filters=None,
                         offset=None):
        response = self.call(
            ctxt, "log_file_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def log_file_create(self, ctxt, data):
        response = self.call(
            ctxt, "log_file_create", data=data)
        return response

    def log_file_get(self, ctxt, log_file_id):
        response = self.call(ctxt, "log_file_get",
                             log_file_id=log_file_id)
        return response

    def log_file_update(self, ctxt, log_file_id, data):
        response = self.call(ctxt, "log_file_update",
                             log_file_id=log_file_id,
                             data=data)
        return response

    def log_file_delete(self, ctxt, log_file_id):
        response = self.call(ctxt, "log_file_delete",
                             log_file_id=log_file_id)
        return response

    ###################

    def volume_snapshot_get_all(self, ctxt, marker=None, limit=None,
                                sort_keys=None, sort_dirs=None, filters=None,
                                offset=None):
        response = self.call(
            ctxt, "volume_snapshot_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def volume_snapshot_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_snapshot_create", data=data)
        return response

    def volume_snapshot_get(self, ctxt, volume_snapshot_id):
        response = self.call(ctxt, "volume_snapshot_get",
                             volume_snapshot_id=volume_snapshot_id)
        return response

    def volume_snapshot_update(self, ctxt, volume_snapshot_id, data):
        response = self.call(ctxt, "volume_snapshot_update",
                             volume_snapshot_id=volume_snapshot_id,
                             data=data)
        return response

    def volume_snapshot_delete(self, ctxt, volume_snapshot_id):
        response = self.call(ctxt, "volume_snapshot_delete",
                             volume_snapshot_id=volume_snapshot_id)
        return response

    def volume_create_from_snapshot(self, ctxt, volume_snapshot_id, data):
        response = self.call(ctxt, "volume_create_from_snapshot",
                             volume_snapshot_id=volume_snapshot_id,
                             data=data)
        return response

    ###################

    def smtp_get(self, ctxt):
        response = self.call(ctxt, "smtp_get")
        return response

    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "ceph_config_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def ceph_config_set(self, ctxt, values):
        response = self.call(ctxt, "ceph_config_set", values=values)
        return response

    def ceph_cluster_info(self, ctxt):
        response = self.call(ctxt, "ceph_cluster_info")
        return response

    ###################


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
    re = client.cluster_install_agent(
        ctxt, ip_address="192.168.211.128", password='aaaaaa'
    )
    print(re)
