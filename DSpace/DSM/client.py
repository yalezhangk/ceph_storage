from __future__ import print_function

import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClient
from DSpace.service import BaseClientManager


class AdminClient(BaseClient):

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None,
                       expected_attrs=None):
        response = self.call(
            ctxt, "volume_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def volume_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "volume_get_count", filters=filters)
        return response

    def email_group_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "email_group_get_count", filters=filters)
        return response

    def volume_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_create", data=data)
        return response

    def volume_get(self, ctxt, volume_id, expected_attrs=None):
        response = self.call(ctxt, "volume_get", volume_id=volume_id,
                             expected_attrs=expected_attrs)
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

    def volume_unlink(self, ctxt, volume_id):
        response = self.call(ctxt, "volume_unlink", volume_id=volume_id)
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

    def cluster_metrics_get(self, ctxt):
        response = self.call(ctxt, "cluster_metrics_get")
        return response

    def cluster_history_metrics_get(self, ctxt, start, end):
        response = self.call(
            ctxt, "cluster_history_metrics_get", start=start, end=end)
        return response

    ###################

    def alert_rule_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "alert_rule_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def alert_rule_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "alert_rule_get_count", filters=filters)
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

    ###################

    def node_get_infos(self, ctxt, data):
        response = self.call(ctxt, "node_get_infos", data=data)
        return response

    def node_check(self, ctxt, data):
        response = self.call(ctxt, "node_check", data=data)
        return response

    def node_roles_set(self, ctxt, node_id, data):
        response = self.call(ctxt, "node_roles_set", node_id=node_id,
                             data=data)
        return response

    def node_get(self, ctxt, node_id, **kwargs):
        response = self.call(ctxt, "node_get", node_id=node_id, **kwargs)
        return response

    def node_create(self, ctxt, data):
        response = self.call(ctxt, "node_create", data=data)
        return response

    def node_update(self, ctxt, node_id, data):
        response = self.call(ctxt, "node_update", node_id=node_id, data=data)
        return response

    def node_update_rack(self, ctxt, node_id, rack_id):
        response = self.call(ctxt, "node_update_rack", node_id=node_id,
                             rack_id=rack_id)
        return response

    def node_delete(self, ctxt, node_id):
        response = self.call(ctxt, "node_delete", node_id=node_id)
        return response

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        response = self.call(
            ctxt, "node_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def node_get_count(self, ctxt, filters=None):
        response = self.call(ctxt, "node_get_count", filters=filters)
        return response

    def node_metrics_monitor_get(self, ctxt, node_id):
        response = self.call(ctxt, "node_metrics_monitor_get", node_id=node_id)
        return response

    def node_metrics_histroy_monitor_get(self, ctxt, node_id, start, end):
        response = self.call(ctxt, "node_metrics_histroy_monitor_get",
                             node_id=node_id, start=start, end=end)
        return response

    def node_metrics_network_get(self, ctxt, node_id, net_name):
        response = self.call(ctxt, "node_metrics_network_get", node_id=node_id,
                             net_name=net_name)
        return response

    def node_metrics_histroy_network_get(
            self, ctxt, node_id, net_name, start, end):
        response = self.call(
            ctxt, "node_metrics_histroy_network_get", node_id=node_id,
            net_name=net_name, start=start, end=end)
        return response

    ###################

    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None, **kwargs):
        response = self.call(
            ctxt, "network_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, **kwargs)
        return response

    def network_get_count(self, ctxt, filters=None):
        response = self.call(ctxt, "network_get_count", filters=filters)
        return response

    def sysconf_get_all(self, ctxt):
        response = self.call(ctxt, "sysconf_get_all")
        return response

    def update_chrony(self, ctxt, chrony_server):
        response = self.call(ctxt, "update_chrony",
                             chrony_server=chrony_server)
        return response

    def cluster_create(self, ctxt, data):
        response = self.call(ctxt, "cluster_create", data=data)
        return response

    def update_sysinfo(self, ctxt, cluster_name, admin_cidr, public_cidr,
                       cluster_cidr, gateway_cidr):
        response = self.call(
            ctxt, "update_sysinfo", cluster_name=cluster_name,
            admin_cidr=admin_cidr, public_cidr=public_cidr,
            cluster_cidr=cluster_cidr, gateway_cidr=gateway_cidr)
        return response

    ###################
    def osd_get(self, ctxt, osd_id, **kwargs):
        response = self.call(ctxt, "osd_get", osd_id=osd_id, **kwargs)
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

    def osd_get_all(self, ctxt, tab=None, marker=None, limit=None,
                    sort_keys=None, sort_dirs=None, filters=None, offset=None,
                    **kwargs):
        response = self.call(
            ctxt, "osd_get_all", tab=tab, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, **kwargs)
        return response

    def osd_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "osd_get_count", filters=filters)
        return response

    def osd_metrics_get(self, ctxt, osd_id):
        response = self.call(ctxt, "osd_metrics_get", osd_id=osd_id)
        return response

    def osd_metrics_history_get(self, ctxt, osd_id, start, end):
        response = self.call(
            ctxt, "osd_metrics_history_get", osd_id=osd_id,
            start=start, end=end)
        return response

    def osd_disk_metrics_get(self, ctxt, osd_id):
        response = self.call(ctxt, "osd_disk_metrics_get", osd_id=osd_id)
        return response

    def osd_history_disk_metrics_get(self, ctxt, osd_id, start, end):
        response = self.call(
            ctxt, "osd_history_disk_metrics_get", osd_id=osd_id,
            start=start, end=end)
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

    def datacenter_tree(self, ctxt):
        response = self.call(ctxt, "datacenter_tree")
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

    def disk_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "disk_get_count", filters=filters)
        return response

    def disk_update(self, ctxt, disk_id, disk_type):
        response = self.call(
            ctxt, "disk_update", disk_id=disk_id, disk_type=disk_type)
        return response

    def disk_light(self, ctxt, disk_id, led):
        response = self.call(ctxt, "disk_light", disk_id=disk_id, led=led)
        return response

    def disk_partitions_create(self, ctxt, disk_id, values):
        response = self.call(ctxt, "disk_partitions_create", disk_id=disk_id,
                             values=values)
        return response

    def disk_partitions_remove(self, ctxt, disk_id, values):
        response = self.call(ctxt, "disk_partitions_remove", disk_id=disk_id,
                             values=values)
        return response

    def disk_smart_get(self, ctxt, disk_id):
        response = self.call(ctxt, "disk_smart_get", disk_id=disk_id)
        return response

    def disk_partition_get_all(self, ctxt, marker=None, limit=None,
                               sort_keys=None, sort_dirs=None, filters=None,
                               offset=None, expected_attrs=None):
        response = self.call(ctxt, "disk_partition_get_all", marker=marker,
                             limit=limit, sort_keys=sort_keys,
                             sort_dirs=sort_dirs, filters=filters,
                             offset=offset, expected_attrs=expected_attrs)
        return response

    def disk_partition_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "disk_partition_get_count", filters=filters)
        return response

    def disk_perf_get(self, ctxt, disk_id):
        response = self.call(ctxt, "disk_perf_get", disk_id=disk_id)
        return response

    def disk_perf_history_get(self, ctxt, disk_id, start, end):
        response = self.call(ctxt, "disk_perf_history_get", disk_id=disk_id,
                             start=start, end=end)
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
                            offset=None, expected_attrs=None):
        response = self.call(
            ctxt, "alert_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def alert_group_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "alert_group_get_count", filters=filters)
        return response

    def alert_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "alert_group_create", data=data)
        return response

    def alert_group_get(self, ctxt, alert_group_id, expected_attrs=None):
        response = self.call(ctxt, "alert_group_get",
                             alert_group_id=alert_group_id,
                             expected_attrs=expected_attrs)
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

    def service_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "service_get_count", filters=filters)
        return response

    def service_update(self, ctxt, services):
        response = self.call(ctxt, "service_update", services=services)
        return response

    ##################

    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None, tab=None):
        response = self.call(
            ctxt, "pool_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs, tab=tab)
        return response

    def pool_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "pool_get_count", filters=filters)
        return response

    def pool_get(self, ctxt, pool_id, expected_attrs=None):
        response = self.call(
            ctxt, "pool_get", pool_id=pool_id, expected_attrs=expected_attrs)
        return response

    def pool_osds_get(self, ctxt, pool_id, expected_attrs=None):
        response = self.call(
            ctxt, "pool_osds_get", pool_id=pool_id,
            expected_attrs=expected_attrs)
        return response

    def pool_create(self, ctxt, data):
        response = self.call(ctxt, "pool_create", data=data)
        return response

    def pool_delete(self, ctxt, pool_id):
        response = self.call(ctxt, "pool_delete", pool_id=pool_id)
        return response

    def pool_update_display_name(self, ctxt, pool_id, pool_name):
        response = self.call(ctxt, "pool_update_display_name",
                             id=pool_id, name=pool_name)
        return response

    def pool_increase_disk(self, ctxt, pool_id, data):
        response = self.call(ctxt, "pool_increase_disk",
                             id=pool_id, data=data)
        return response

    def pool_decrease_disk(self, ctxt, pool_id, data):
        response = self.call(ctxt, "pool_decrease_disk",
                             id=pool_id, data=data)
        return response

    def pool_update_policy(self, ctxt, pool_id, data):
        response = self.call(ctxt, "pool_update_policy",
                             id=pool_id, data=data)
        return response

    def pool_metrics_get(self, ctxt, pool_id):
        response = self.call(ctxt, "pool_metrics_get", pool_id=pool_id)
        return response

    def pool_metrics_history_get(self, ctxt, pool_id, start, end):
        response = self.call(
            ctxt, "pool_metrics_history_get", pool_id=pool_id,
            start=start, end=end)
        return response

    ##################

    def volume_access_path_get_all(self, ctxt, marker=None, limit=None,
                                   sort_keys=None, sort_dirs=None,
                                   filters=None, offset=None,
                                   expected_attrs=None):
        response = self.call(
            ctxt, "volume_access_path_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def volume_access_path_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "volume_access_path_get_count", filters=filters)
        return response

    def volume_access_path_get(
            self, ctxt, volume_access_path_id, expected_attrs=None):
        response = self.call(ctxt, "volume_access_path_get",
                             volume_access_path_id=volume_access_path_id,
                             expected_attrs=expected_attrs)
        return response

    def volume_access_path_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_access_path_create", data=data)
        return response

    def volume_access_path_update(self, ctxt, id, data):
        response = self.call(ctxt, "volume_access_path_update",
                             id=id, data=data)
        return response

    def volume_access_path_delete(self, ctxt, id):
        response = self.call(ctxt, "volume_access_path_delete", id=id)
        return response

    ##################

    def volume_client_group_get_all(self, ctxt, marker=None, limit=None,
                                    sort_keys=None, sort_dirs=None,
                                    filters=None, offset=None,
                                    expected_attrs=None):
        response = self.call(
            ctxt, "volume_client_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def volume_client_group_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "volume_client_group_get_count", filters=filters)
        return response

    def volume_client_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_client_group_create", data=data)
        return response

    def volume_client_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_client_create", data=data)
        return response

    def volume_client_group_get(self, ctxt, group_id, expected_attrs=None):
        response = self.call(
            ctxt, "volume_client_group_get", group_id=group_id,
            expected_attrs=expected_attrs)
        return response

    def volume_client_group_delete(self, ctxt, group_id):
        response = self.call(
            ctxt, "volume_client_group_delete", group_id=group_id)
        return response

    def volume_client_get_all(self, ctxt, marker=None, limit=None,
                              sort_keys=None, sort_dirs=None,
                              filters=None, offset=None):
        response = self.call(
            ctxt, "volume_client_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
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

    def alert_log_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "alert_log_get_count", filters=filters)
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

    def action_log_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "action_log_get_count", filters=filters)
        return response

    def resource_action(self, ctxt):
        response = self.call(
            ctxt, "resource_action")
        return response

    ###################

    def log_file_get_all(self, ctxt, node_id, service_type, marker=None,
                         limit=None, sort_keys=None, sort_dirs=None,
                         filters=None, offset=None):
        response = self.call(
            ctxt, "log_file_get_all", node_id=node_id,
            service_type=service_type, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs,
            filters=filters, offset=offset)
        return response

    def log_file_get_count(self, ctxt, node_id, service_type, filters=None):
        response = self.call(
            ctxt, "log_file_get_count", node_id=node_id,
            service_type=service_type, filters=filters)
        return response

    def log_file_create(self, ctxt, data):
        response = self.call(
            ctxt, "log_file_create", data=data)
        return response

    def log_file_get(self, ctxt, log_file_id):
        response = self.call(ctxt, "log_file_get", log_file_id=log_file_id)
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
                                offset=None, expected_attrs=None):
        response = self.call(
            ctxt, "volume_snapshot_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def volume_snapshot_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "volume_snapshot_get_count", filters=filters)
        return response

    def volume_snapshot_create(self, ctxt, data):
        response = self.call(
            ctxt, "volume_snapshot_create", data=data)
        return response

    def volume_snapshot_get(self, ctxt, volume_snapshot_id, expected_attrs):
        response = self.call(ctxt, "volume_snapshot_get",
                             volume_snapshot_id=volume_snapshot_id,
                             expected_attrs=expected_attrs)
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

    def volume_create_from_snapshot(self, ctxt, snapshot_id, data):
        response = self.call(ctxt, "volume_create_from_snapshot",
                             snapshot_id=snapshot_id,
                             data=data)
        return response

    ###################

    def smtp_get(self, ctxt):
        response = self.call(ctxt, "smtp_get")
        return response

    def update_smtp(self, ctxt, smtp_enabled, smtp_user,
                    smtp_password, smtp_host, smtp_port,
                    smtp_enable_ssl, smtp_enable_tls):
        response = self.call(
            ctxt, "update_smtp", smtp_enabled=smtp_enabled,
            smtp_user=smtp_user, smtp_password=smtp_password,
            smtp_host=smtp_host, smtp_port=smtp_port,
            smtp_enable_ssl=smtp_enable_ssl,
            smtp_enable_tls=smtp_enable_tls)
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

    def ceph_config_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "ceph_config_get_count", filters=filters)
        return response

    def ceph_config_content(self, ctxt):
        response = self.call(ctxt, "ceph_config_content")
        return response

    def ceph_cluster_info(self, ctxt):
        response = self.call(ctxt, "ceph_cluster_info")
        return response

    ###################

    def action_log_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        response = self.call(
            ctxt, "action_log_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def action_log_create(self, ctxt, data):
        response = self.call(
            ctxt, "action_log_create", data=data)
        return response

    def action_log_get(self, ctxt, action_log_id):
        response = self.call(ctxt, "action_log_get",
                             action_log_id=action_log_id)
        return response

    def action_log_update(self, ctxt, action_log_id, data):
        response = self.call(ctxt, "action_log_update",
                             action_log_id=action_log_id,
                             data=data)
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
    re = client.cluster_install_agent(
        ctxt, ip_address="192.168.211.128", password='aaaaaa'
    )
    print(re)
