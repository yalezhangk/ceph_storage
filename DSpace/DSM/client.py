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

    def email_group_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "email_group_get_count", filters=filters)
        return response

    ###################

    def cluster_get(self, ctxt, cluster_id):
        response = self.call(ctxt, "cluster_get", cluster_id=cluster_id)
        return response

    def cluster_create(self, ctxt, data):
        response = self.call(ctxt, "cluster_create", data=data)
        return response

    def cluster_delete(self, ctxt, cluster_id, clean_ceph=False):
        response = self.call(ctxt, "cluster_delete",
                             cluster_id=cluster_id, clean_ceph=clean_ceph)
        return response

    def cluster_get_info(self, ctxt, ip_address, password=None):
        response = self.call(
            ctxt, "cluster_get_info", ip_address=ip_address,
            password=password)
        return response

    def cluster_install_agent(self, ctxt, ip_address, password=None):
        response = self.call(ctxt, "cluster_install_agent",
                             ip_address=ip_address, password=password)
        return response

    def cluster_platform_check(self, ctxt):
        response = self.call(ctxt, "cluster_platform_check")
        return response

    def check_admin_node_status(self, ctxt):
        response = self.call(ctxt, "check_admin_node_status")
        return response

    def cluster_admin_nodes_get(self, ctxt):
        response = self.call(ctxt, "cluster_admin_nodes_get")
        return response

    def cluster_metrics_get(self, ctxt):
        response = self.call(ctxt, "cluster_metrics_get")
        return response

    def cluster_history_metrics_get(self, ctxt, start, end):
        response = self.call(
            ctxt, "cluster_history_metrics_get", start=start, end=end)
        return response

    def cluster_host_status_get(self, ctxt):
        response = self.call(ctxt, "cluster_host_status_get")
        return response

    def cluster_pool_status_get(self, ctxt):
        response = self.call(ctxt, "cluster_pool_status_get")
        return response

    def cluster_osd_status_get(self, ctxt):
        response = self.call(ctxt, "cluster_osd_status_get")
        return response

    def cluster_capacity_status_get(self, ctxt):
        response = self.call(ctxt, "cluster_capacity_status_get")
        return response

    def cluster_pg_status_get(self, ctxt):
        response = self.call(ctxt, "cluster_pg_status_get")
        return response

    def cluster_switch(self, ctxt, cluster_id):
        response = self.call(ctxt, "cluster_switch", cluster_id=cluster_id)
        return response

    def cluster_capacity_get(self, ctxt, pool_id=None):
        response = self.call(ctxt, "cluster_capacity_get", pool_id=pool_id)
        return response

    def cluster_pause(self, ctxt, enable=True):
        response = self.call(ctxt, "cluster_pause", enable=enable)
        return response

    def cluster_status(self, ctxt):
        response = self.call(ctxt, "cluster_status")
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

    def alert_rule_get(self, ctxt, alert_rule_id, expected_attrs=None):
        response = self.call(ctxt, "alert_rule_get",
                             alert_rule_id=alert_rule_id,
                             expected_attrs=expected_attrs)
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

    def nodes_inclusion(self, ctxt, datas):
        response = self.call(ctxt, "nodes_inclusion", datas=datas)
        return response

    def nodes_inclusion_clean(self, ctxt):
        response = self.call(ctxt, "nodes_inclusion_clean")
        return response

    def nodes_inclusion_check(self, ctxt, datas):
        response = self.call(ctxt, "nodes_inclusion_check", datas=datas)
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

    def image_namespace_get(self, ctxt):
        response = self.call(ctxt, "image_namespace_get")
        return response

    def update_sysinfo(self, ctxt, sysinfos):
        response = self.call(ctxt, "update_sysinfo", sysinfos=sysinfos)
        return response

    def network_reporter(self, ctxt, networks, node_id):
        response = self.call(ctxt, "network_reporter",
                             networks=networks, node_id=node_id)
        return response

    def node_reporter(self, ctxt, node_summary, node_id):
        response = self.call(ctxt, "node_reporter",
                             node_summary=node_summary, node_id=node_id)
        return response

    def disk_io_top(self, ctxt, k=None):
        response = self.call(ctxt, "disk_io_top", k=k)
        return response

    ###################
    def osd_get(self, ctxt, osd_id, **kwargs):
        response = self.call(ctxt, "osd_get", osd_id=osd_id, **kwargs)
        return response

    def osd_create(self, ctxt, data):
        response = self.call(ctxt, "osd_create", data=data)
        return response

    def get_disk_info(self, ctxt, data):
        response = self.call(ctxt, "get_disk_info", data=data)
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

    def osd_capacity_get(self, ctxt, osd_id):
        response = self.call(ctxt, "osd_capacity_get", osd_id=osd_id)
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

    def datacenter_get_all(self, ctxt, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None,
                           filters=None, offset=None):
        response = self.call(ctxt, "datacenter_get_all", marker=marker,
                             limit=limit, sort_keys=sort_keys,
                             sort_dirs=sort_dirs, filters=filters,
                             offset=offset)
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

    def rack_get(self, ctxt, rack_id, **kwargs):
        response = self.call(ctxt, "rack_get", rack_id=rack_id, **kwargs)
        return response

    def rack_delete(self, ctxt, rack_id):
        response = self.call(ctxt, "rack_delete",
                             rack_id=rack_id)
        return response

    def rack_get_all(self, ctxt, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None,
                     filters=None, offset=None, expected_attrs=None):
        response = self.call(ctxt, "rack_get_all", marker=marker,
                             limit=limit, sort_keys=sort_keys,
                             sort_dirs=sort_dirs, filters=filters,
                             offset=offset, expected_attrs=expected_attrs)
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

    def disk_get_all(self, ctxt, tab=None, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        response = self.call(
            ctxt, "disk_get_all", tab=tab, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def disk_get_all_available(self, ctxt, filters=None, expected_attrs=None):
        response = self.call(
            ctxt, "disk_get_all_available", filters=filters,
            expected_attrs=expected_attrs)
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

    def disk_partition_get_all_available(self, ctxt, filters=None,
                                         expected_attrs=None):
        response = self.call(ctxt, "disk_partition_get_all_available",
                             filters=filters, expected_attrs=expected_attrs)
        return response

    def disk_perf_get(self, ctxt, disk_id):
        response = self.call(ctxt, "disk_perf_get", disk_id=disk_id)
        return response

    def disk_perf_history_get(self, ctxt, disk_id, start, end):
        response = self.call(ctxt, "disk_perf_history_get", disk_id=disk_id,
                             start=start, end=end)
        return response

    def disk_reporter(self, ctxt, disks, node_id):
        response = self.call(ctxt, "disk_reporter", disks=disks,
                             node_id=node_id)
        return response

    ###################

    def email_group_get_all(self, ctxt, marker=None, limit=None,
                            sort_keys=None, sort_dirs=None, filters=None,
                            offset=None, expected_attrs=None):
        response = self.call(
            ctxt, "email_group_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def email_group_create(self, ctxt, data):
        response = self.call(
            ctxt, "email_group_create", data=data)
        return response

    def email_group_get(self, ctxt, email_group_id, expected_attrs=None):
        response = self.call(ctxt, "email_group_get",
                             email_group_id=email_group_id,
                             expected_attrs=expected_attrs)
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

    def service_status_get(self, ctxt, names):
        response = self.call(ctxt, "service_status_get", names=names)
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

    def pool_capacity_get(self, ctxt, pool_id):
        response = self.call(
            ctxt, "pool_capacity_get", pool_id=pool_id)
        return response

    def pool_osd_tree(self, ctxt, pool_id):
        response = self.call(
            ctxt, "pool_osd_tree", pool_id=pool_id)
        return response

    def pool_undo(self, ctxt):
        response = self.call(ctxt, "pool_undo")
        return response

    def pool_get_undo(self, ctxt):
        response = self.call(ctxt, "pool_get_undo")
        return response

    ##################

    def alert_log_get_all(self, ctxt, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None, filters=None,
                          offset=None, expected_attrs=None):
        response = self.call(
            ctxt, "alert_log_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def alert_log_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "alert_log_get_count", filters=filters)
        return response

    def alert_log_get(self, ctxt, alert_log_id, expected_attrs):
        response = self.call(ctxt, "alert_log_get",
                             alert_log_id=alert_log_id,
                             expected_attrs=expected_attrs)
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

    def send_alert_messages(self, ctxt, receive_datas):
        response = self.call(
            ctxt, "send_alert_messages", receive_datas=receive_datas)
        return response

    def alert_log_all_readed(self, ctxt, alert_log_data):
        response = self.call(
            ctxt, "alert_log_all_readed", alert_log_data=alert_log_data)
        return response

    def alert_logs_set_deleted(self, ctxt, alert_log_data):
        response = self.call(
            ctxt, "alert_logs_set_deleted", alert_log_data=alert_log_data)
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

    def download_log_file(self, ctxt, log_file_id=None):
        response = self.call(ctxt, "download_log_file",
                             log_file_id=log_file_id)
        return response

    ###################

    ###################

    def smtp_get(self, ctxt):
        response = self.call(ctxt, "smtp_get")
        return response

    def update_smtp(self, ctxt, data):
        response = self.call(
            ctxt, "update_smtp", data=data)
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
                           offset=None, expected_attrs=None):
        response = self.call(
            ctxt, "action_log_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return response

    def action_log_get(self, ctxt, action_log_id, expected_attrs=None):
        response = self.call(ctxt, "action_log_get",
                             action_log_id=action_log_id,
                             expected_attrs=expected_attrs)
        return response

    #####################

    def probe_cluster_nodes(self, ctxt, ip, password, user="root", port=22):
        response = self.call(ctxt, "probe_cluster_nodes",
                             ip=ip, password=password, port=port, user=user)
        return response

    ####################

    def task_get_all(self, ctxt, marker=None, limit=None,
                     sort_keys=None, sort_dirs=None, filters=None,
                     offset=None):
        response = self.call(
            ctxt, "task_get_all",
            marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def task_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "task_get_count", filters=filters)
        return response

    def task_get(self, ctxt, task_id):
        response = self.call(ctxt, "task_get",
                             task_id=task_id)
        return response

    ####################

    def send_mail(self, ctxt, subject, content, config):
        response = self.call(ctxt, "send_mail", subject=subject,
                             content=content, config=config)
        return response

    ####################

    def components_get_list(self, ctxt, services):
        response = self.call(ctxt, "components_get_list", services=services)
        return response

    def component_restart(self, ctxt, component):
        response = self.call(ctxt, "component_restart", component=component)
        return response

    def component_start(self, ctxt, component):
        response = self.call(ctxt, "component_start", component=component)
        return response

    def component_stop(self, ctxt, component):
        response = self.call(ctxt, "component_stop", component=component)
        return response

    ####################

    def radosgw_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "radosgw_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def radosgw_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "radosgw_get_count", filters=filters)
        return response

    def radosgw_create(self, ctxt, data):
        response = self.call(
            ctxt, "radosgw_create", data=data)
        return response

    def radosgw_delete(self, ctxt, rgw_id):
        response = self.call(ctxt, "radosgw_delete", rgw_id=rgw_id)
        return response

    ####################

    def cluster_data_balance_get(self, ctxt):
        response = self.call(ctxt, "cluster_data_balance_get")
        return response

    def cluster_data_balance_set(self, ctxt, data_balance):
        response = self.call(ctxt, "cluster_data_balance_set",
                             data_balance=data_balance)
        return response

    def rgw_router_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        response = self.call(
            ctxt, "rgw_router_get_all", marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return response

    def rgw_router_get_count(self, ctxt, filters=None):
        response = self.call(
            ctxt, "rgw_router_get_count", filters=filters)
        return response

    def rgw_router_create(self, ctxt, data):
        response = self.call(
            ctxt, "rgw_router_create", data=data)
        return response

    def rgw_router_delete(self, ctxt, rgw_router_id):
        response = self.call(ctxt, "rgw_router_delete",
                             rgw_router_id=rgw_router_id)
        return response

    def rgw_router_update(self, ctxt, rgw_router_id, data):
        response = self.call(ctxt, "rgw_router_update",
                             rgw_router_id=rgw_router_id, data=data)
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
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False,
                          request_id='xxxxxxxxxxxxxxxxxxxxxxxxx')
    logger = logging.getLogger(__name__)
    logger.info('Simple')
