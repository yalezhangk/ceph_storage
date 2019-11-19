from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import options as db_options

from DSpace.common import constants
from DSpace.i18n import _

CONF = cfg.CONF
db_options.set_defaults(CONF)

_BACKEND_MAPPING = {'sqlalchemy': 'DSpace.db.sqlalchemy.api'}


IMPL = oslo_db_api.DBAPI.from_config(conf=CONF,
                                     backend_mapping=_BACKEND_MAPPING,
                                     lazy=True)

# The maximum value a signed INT type may have
MAX_INT = constants.DB_MAX_INT


###################

def dispose_engine():
    """Force the engine to establish new connections."""

    # FIXME(jdg): When using sqlite if we do the dispose
    # we seem to lose our DB here.  Adding this check
    # means we don't do the dispose, but we keep our sqlite DB
    # This likely isn't the best way to handle this

    if 'sqlite' not in IMPL.get_engine().name:
        return IMPL.dispose_engine()
    else:
        return

###############


def sys_config_create(context, values):
    return IMPL.sys_config_create(context, values)


def sys_config_destroy(context, sys_config_id):
    return IMPL.sys_config_destroy(context, sys_config_id)


def sys_config_get(context, sys_config_id):
    return IMPL.sys_config_get(context, sys_config_id)


def sys_config_get_by_key(context, key, cluster_id):
    return IMPL.sys_config_get_by_key(context, key, cluster_id)


def sys_config_get_all(context, filters, marker, limit,
                       offset, sort_keys, sort_dirs):
    return IMPL.sys_config_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def sys_config_update(context, sys_config_id, values):
    return IMPL.sys_config_update(context, sys_config_id, values)


###############


def volume_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_create(context, values)


def volume_destroy(context, volume_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_destroy(context, volume_id)


def volume_get(context, volume_id):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_get(context, volume_id)


def volume_get_all(context, marker=None, limit=None, sort_keys=None,
                   sort_dirs=None, filters=None, offset=None,
                   expected_attrs=None):
    """Get all volumes."""
    return IMPL.volume_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
        offset=offset, expected_attrs=expected_attrs)


def volume_get_count(context, filters):
    return IMPL.volume_get_count(context, filters=filters)


def volume_update(context, volume_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_update(context, volume_id, values)


def volumes_update(context, values_list):
    """Set the given properties on a list of volumes and update them.

    Raises NotFound if a volume does not exist.
    """
    return IMPL.volumes_update(context, values_list)


###############


def cluster_create(context, values):
    return IMPL.cluster_create(context, values)


def cluster_destroy(context, cluster_id):
    return IMPL.cluster_destroy(context, cluster_id)


def cluster_get(context, cluster_id):
    return IMPL.cluster_get(context, cluster_id)


def cluster_get_all(context, filters, marker, limit,
                    offset, sort_keys, sort_dirs):
    return IMPL.cluster_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def cluster_update(context, cluster_id, values):
    return IMPL.cluster_update(context, cluster_id, values)


def clusters_update(context, values_list):
    return IMPL.clusters_update(context, values_list)


def node_status_get(context):
    return IMPL.node_status_get(context)


def pool_status_get(context):
    return IMPL.pool_status_get(context)


def osd_status_get(context):
    return IMPL.osd_status_get(context)

##################


def rpc_service_create(context, values):
    return IMPL.rpc_service_create(context, values)


def rpc_service_destroy(context, rpc_service_id):
    return IMPL.rpc_service_destroy(context, rpc_service_id)


def rpc_service_get(context, rpc_service_id):
    return IMPL.rpc_service_get(context, rpc_service_id)


def service_status_get(context, names):
    return IMPL.service_status_get(context, names=names)


def rpc_service_get_all(context, filters, marker, limit,
                        offset, sort_keys, sort_dirs):
    return IMPL.rpc_service_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def rpc_service_update(context, rpc_service_id, values):
    return IMPL.rpc_service_update(context, rpc_service_id, values)


###################


def node_create(context, values):
    return IMPL.node_create(context, values)


def node_destroy(context, node_id):
    return IMPL.node_destroy(context, node_id)


def node_get(context, node_id):
    return IMPL.node_get(context, node_id)


def node_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs,
                 expected_attrs=None):
    return IMPL.node_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def node_get_count(context, filters):
    return IMPL.node_get_count(context, filters=filters)


def node_update(context, node_id, values):
    return IMPL.node_update(context, node_id, values)


###################


def pool_create(context, values):
    return IMPL.pool_create(context, values)


def pool_destroy(context, pool_id):
    return IMPL.pool_destroy(context, pool_id)


def pool_get(context, pool_id, expected_attrs=None):
    return IMPL.pool_get(context, pool_id, expected_attrs=expected_attrs)


def pool_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs, expected_attrs=None):
    return IMPL.pool_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def pool_get_count(context, filters):
    return IMPL.pool_get_count(context, filters=filters)


def pool_update(context, pool_id, values):
    return IMPL.pool_update(context, pool_id, values)


###################


def datacenter_create(context, values):
    return IMPL.datacenter_create(context, values)


def datacenter_destroy(context, datacenter_id):
    return IMPL.datacenter_destroy(context, datacenter_id)


def datacenter_get(context, datacenter_id):
    return IMPL.datacenter_get(context, datacenter_id)


def datacenter_get_all(context, filters, marker, limit,
                       offset, sort_keys, sort_dirs):
    return IMPL.datacenter_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def datacenter_update(context, datacenter_id, values):
    return IMPL.datacenter_update(context, datacenter_id, values)


###################


def rack_create(context, values):
    return IMPL.rack_create(context, values)


def rack_destroy(context, rack_id):
    return IMPL.rack_destroy(context, rack_id)


def rack_get(context, rack_id):
    return IMPL.rack_get(context, rack_id)


def rack_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs):
    return IMPL.rack_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def rack_update(context, rack_id, values):
    return IMPL.rack_update(context, rack_id, values)


###################


def osd_create(context, values):
    return IMPL.osd_create(context, values)


def osd_destroy(context, osd_id):
    return IMPL.osd_destroy(context, osd_id)


def osd_get(context, osd_id, expected_attrs=None):
    return IMPL.osd_get(context, osd_id,
                        expected_attrs=expected_attrs)


def osd_get_by_osd_id(context, osd_id, expected_attrs=None):
    return IMPL.osd_get_by_osd_id(
        context, osd_id, expected_attrs=expected_attrs)


def osd_get_all(context, filters, marker, limit,
                offset, sort_keys, sort_dirs,
                expected_attrs=None):
    return IMPL.osd_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def osd_get_count(context, filters):
    return IMPL.osd_get_count(context, filters=filters)


def osd_update(context, osd_id, values):
    return IMPL.osd_update(context, osd_id, values)


def osd_get_by_pool(context, pool_id, expected_attrs=None):
    return IMPL.osd_get_by_pool(
        context, pool_id, expected_attrs=expected_attrs)


###################


def volume_access_path_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_access_path_create(context, values)


def volume_access_path_destroy(context, access_path_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_access_path_destroy(context, access_path_id)


def volume_access_path_get(context, access_path_id, expected_attrs=None):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_access_path_get(
        context, access_path_id, expected_attrs=expected_attrs)


def volume_access_path_get_all(context, marker=None, limit=None,
                               sort_keys=None, sort_dirs=None,
                               filters=None, offset=None,
                               expected_attrs=None):
    """Get all volumes."""
    return IMPL.volume_access_path_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters,
        offset=offset, expected_attrs=expected_attrs)


def volume_access_path_get_count(context, filters):
    return IMPL.volume_access_path_get_count(context, filters=filters)


def volume_access_path_update(context, access_path_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_access_path_update(context, access_path_id, values)


def volume_access_paths_update(context, values_list):
    """Set the given properties on a list of volumes and update them.

    Raises NotFound if a volume does not exist.
    """
    return IMPL.volume_access_paths_update(context, values_list)


def volume_access_path_append_gateway(context, access_path_id,
                                      volume_gateway_id):
    return IMPL.volume_access_path_append_gateway(
        context, access_path_id, volume_gateway_id)


def volume_access_path_remove_gateway(context, access_path_id,
                                      volume_gateway_id):
    return IMPL.volume_access_path_remove_gateway(
        context, access_path_id, volume_gateway_id)
###############


def volume_gateway_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_gateway_create(context, values)


def volume_gateway_destroy(context, gateway_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_gateway_destroy(context, gateway_id)


def volume_gateway_get(context, gateway_id):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_gateway_get(context, gateway_id)


def volume_gateway_get_all(context, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None,
                           filters=None, offset=None):
    """Get all volumes."""
    return IMPL.volume_gateway_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters,
        offset=offset)


def volume_gateway_update(context, gateway_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_gateway_update(context, gateway_id, values)


def volume_gateways_update(context, values_list):
    """Set the given properties on a list of volumes and update them.

    Raises NotFound if a volume does not exist.
    """
    return IMPL.volume_gateways_update(context, values_list)

###############


def volume_client_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_client_create(context, values)


def volume_client_destroy(context, volume_client_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_client_destroy(context, volume_client_id)


def volume_client_get(context, volume_client_id):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_client_get(context, volume_client_id)


def volume_client_get_all(context, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None,
                          filters=None, offset=None):
    """Get all volumes."""
    return IMPL.volume_client_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters,
        offset=offset)


def volume_client_update(context, volume_client_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_client_update(context, volume_client_id, values)


###############


def volume_client_group_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_client_group_create(context, values)


def volume_client_group_destroy(context, client_group_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_client_group_destroy(context, client_group_id)


def volume_client_group_get(context, client_group_id, expected_attrs=None):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_client_group_get(
        context, client_group_id, expected_attrs=expected_attrs)


def volume_client_group_get_all(context, marker=None, limit=None,
                                sort_keys=None, sort_dirs=None,
                                filters=None, offset=None,
                                expected_attrs=None):
    """Get all volumes."""
    return IMPL.volume_client_group_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters,
        offset=offset, expected_attrs=expected_attrs)


def volume_client_group_get_count(context, filters):
    return IMPL.volume_client_group_get_count(context, filters=filters)


def volume_client_group_update(context, client_group_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_client_group_update(context, client_group_id, values)

###############
# Volume Mappings


def volume_mapping_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_mapping_create(context, values)


def volume_mapping_destroy(context, mapping_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_mapping_destroy(context, mapping_id)


def volume_mapping_get(context, mapping_id):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_mapping_get(context, mapping_id)


def volume_mapping_get_all(context, marker=None, limit=None,
                           sort_keys=None, sort_dirs=None,
                           filters=None, offset=None):
    """Get all volume mappings."""
    return IMPL.volume_mapping_get_all(
        context, marker=marker, limit=limit,
        sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters,
        offset=offset)


def volume_mapping_update(context, mapping_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_mapping_update(context, mapping_id, values)


def volume_mappings_update(context, values_list):
    """Set the given properties on a list of volumes and update them.

    Raises NotFound if a volume does not exist.
    """
    return IMPL.volume_mappings_update(context, values_list)

###############


def license_create(context, values):
    return IMPL.license_create(context, values)


def license_update(context, license_id, values):
    return IMPL.license_update(context, license_id, values)


def license_get_latest_valid(context, *args, **kwargs):
    return IMPL.license_get_latest_valid(context, *args, **kwargs)

###############


def network_create(context, values):
    return IMPL.network_create(context, values)


def network_destroy(context, net_id):
    return IMPL.network_destroy(context, net_id)


def network_get(context, net_id):
    return IMPL.network_get(context, net_id)


def network_get_all(context, filters, marker, limit,
                    offset, sort_keys, sort_dirs, expected_attrs=None):
    return IMPL.network_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def network_get_count(context, filters):
    return IMPL.network_get_count(context, filters=filters)


def network_update(context, net_id, values):
    return IMPL.network_update(context, net_id, values)


###############


def alert_rule_update(context, alert_rule_id, values):
    return IMPL.alert_rule_update(context, alert_rule_id, values)


def alert_rule_get_all(context, *args, **kwargs):
    return IMPL.alert_rule_get_all(context, *args, **kwargs)


def alert_rule_create(context, values):
    return IMPL.alert_rule_create(context, values)


def alert_rule_destroy(context, alert_rule_id):
    return IMPL.alert_rule_destroy(context, alert_rule_id)


def alert_rule_get_count(context, filters):
    return IMPL.alert_rule_get_count(context, filters=filters)


###############


def disk_create(context, values):
    return IMPL.disk_create(context, values)


def disk_destroy(context, disk_id):
    return IMPL.disk_destroy(context, disk_id)


def disk_get(context, disk_id):
    return IMPL.disk_get(context, disk_id)


def disk_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs,
                 expected_attrs=None):
    return IMPL.disk_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def disk_get_count(context, filters):
    return IMPL.disk_get_count(context, filters=filters)


def disk_update(context, disk_id, values):
    return IMPL.disk_update(context, disk_id, values)


def disk_get_all_available(context, filters=None, expected_attrs=None):
    return IMPL.disk_get_all_available(context, filters,
                                       expected_attrs=expected_attrs)


###################


def disk_partition_create(context, values):
    return IMPL.disk_partition_create(context, values)


def disk_partition_destroy(context, disk_part_id):
    return IMPL.disk_partition_destroy(context, disk_part_id)


def disk_partition_get(context, disk_part_id):
    return IMPL.disk_partition_get(context, disk_part_id)


def disk_partition_get_all(context, filters, marker, limit,
                           offset, sort_keys, sort_dirs,
                           expected_attrs=None):
    return IMPL.disk_partition_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def disk_partition_get_count(context, filters):
    return IMPL.disk_partition_get_count(context, filters=filters)


def disk_partition_update(context, disk_part_id, values):
    return IMPL.disk_partition_update(context, disk_part_id, values)


def disk_partition_get_all_available(context, filters=None,
                                     expected_attrs=None):
    return IMPL.disk_partition_get_all_available(context, filters,
                                                 expected_attrs=expected_attrs)

###################


def email_group_create(context, values):
    return IMPL.email_group_create(context, values)


def email_group_update(context, alert_rule_id, values):
    return IMPL.email_group_update(context, alert_rule_id, values)


def email_group_get_all(context, *args, **kwargs):
    return IMPL.email_group_get_all(context, *args, **kwargs)


def email_group_get_count(context, filters):
    return IMPL.email_group_get_count(context, filters=filters)


def email_group_destroy(context, email_group_id):
    return IMPL.email_group_destroy(context, email_group_id)


###############


def alert_group_create(context, values):
    return IMPL.alert_group_create(context, values)


def alert_group_update(context, alert_rule_id, values):
    return IMPL.alert_group_update(context, alert_rule_id, values)


def alert_group_get_all(context, *args, **kwargs):
    return IMPL.alert_group_get_all(context, *args, **kwargs)


def alert_group_destroy(context, email_group_id):
    return IMPL.alert_group_destroy(context, email_group_id)


def alert_group_get_count(context, filters):
    return IMPL.alert_group_get_count(context, filters=filters)


###############


def alert_log_create(context, values):
    return IMPL.alert_log_create(context, values)


def alert_log_update(context, alert_log_id, values):
    return IMPL.alert_log_update(context, alert_log_id, values)


def alert_log_get_all(context, *args, **kwargs):
    return IMPL.alert_log_get_all(context, *args, **kwargs)


def alert_log_destroy(context, alert_log_id):
    return IMPL.alert_log_destroy(context, alert_log_id)


def alert_log_get_count(context, filters):
    return IMPL.alert_log_get_count(context, filters=filters)


def alert_log_batch_update(context, filters, updates):
    return IMPL.alert_log_batch_update(context, filters, updates)


###############


def log_file_create(context, values):
    return IMPL.log_file_create(context, values)


def log_file_update(context, log_file_id, values):
    return IMPL.log_file_update(context, log_file_id, values)


def log_file_get_all(context, *args, **kwargs):
    return IMPL.log_file_get_all(context, *args, **kwargs)


def log_file_get_count(context, filters):
    return IMPL.log_file_get_count(context, filters=filters)


def log_file_destroy(context, log_file_id):
    return IMPL.log_file_destroy(context, log_file_id)


###############


def volume_snapshot_create(context, values):
    return IMPL.volume_snapshot_create(context, values)


def volume_snapshot_update(context, volume_snapshot_id, values):
    return IMPL.volume_snapshot_update(context, volume_snapshot_id, values)


def volume_snapshot_get_all(context, *args, **kwargs):
    return IMPL.volume_snapshot_get_all(context, *args, **kwargs)


def volume_snapshot_get_count(context, filters):
    return IMPL.volume_snapshot_get_count(context, filters=filters)


def volume_snapshot_destroy(context, volume_snapshot_id):
    return IMPL.volume_snapshot_destroy(context, volume_snapshot_id)


###############


def service_create(context, values):
    return IMPL.service_create(context, values)


def service_destroy(context, service_id):
    return IMPL.service_destroy(context, service_id)


def service_get(context, service_id):
    return IMPL.service_get(context, service_id)


def service_get_all(context, filters, marker, limit,
                    offset, sort_keys, sort_dirs):
    return IMPL.service_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def service_get_count(context, filters):
    return IMPL.service_get_count(context, filters=filters)


def service_update(context, service_id, values):
    return IMPL.service_update(context, service_id, values)


###################


def ceph_config_create(context, values):
    return IMPL.ceph_config_create(context, values)


def ceph_config_destroy(context, ceph_config_id):
    return IMPL.ceph_config_destroy(context, ceph_config_id)


def ceph_config_get(context, ceph_config_id):
    return IMPL.ceph_config_get(context, ceph_config_id)


def ceph_config_get_all(context, filters, marker, limit,
                        offset, sort_keys, sort_dirs):
    return IMPL.ceph_config_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset)


def ceph_config_get_count(context, filters):
    return IMPL.ceph_config_get_count(context, filters=filters)


def ceph_config_update(context, ceph_config_id, values):
    return IMPL.ceph_config_update(context, ceph_config_id, values)


def ceph_config_get_by_key(context, group, key):
    return IMPL.ceph_config_get_by_key(context, group, key)


###############


def crush_rule_create(context, values):
    return IMPL.crush_rule_create(context, values)


def crush_rule_destroy(context, crush_rule_id):
    return IMPL.crush_rule_destroy(context, crush_rule_id)


def crush_rule_get(context, crush_rule_id):
    return IMPL.crush_rule_get(context, crush_rule_id)


def crush_rule_get_all(context, filters, marker, limit,
                       offset, sort_keys, sort_dirs,
                       expected_attrs=None):
    return IMPL.crush_rule_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def crush_rule_update(context, crush_rule_id, values):
    return IMPL.crush_rule_update(context, crush_rule_id, values)


###################


def action_log_create(context, values):
    return IMPL.action_log_create(context, values)


def action_log_destroy(context, action_log_id):
    return IMPL.node_destroy(context, action_log_id)


def action_log_update(context, action_log_id, values):
    return IMPL.action_log_update(context, action_log_id, values)


def action_log_get_all(context, *args, **kwargs):
    return IMPL.action_log_get_all(context, *args, **kwargs)


def action_log_get_count(context, filters):
    return IMPL.action_log_get_count(context, filters=filters)


###############


def user_create(context, values):
    return IMPL.user_create(context, values)


def user_destroy(context, user_id):
    return IMPL.user_destroy(context, user_id)


def user_get(context, user_id):
    return IMPL.user_get(context, user_id)


def user_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs,
                 expected_attrs=None):
    return IMPL.user_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def user_get_count(context, filters):
    return IMPL.user_get_count(context, filters=filters)


def user_update(context, user_id, values):
    return IMPL.user_update(context, user_id, values)


###############


def task_create(context, values):
    return IMPL.task_create(context, values)


def task_destroy(context, task_id):
    return IMPL.task_destroy(context, task_id)


def task_get(context, task_id):
    return IMPL.task_get(context, task_id)


def task_get_all(context, filters, marker, limit,
                 offset, sort_keys, sort_dirs,
                 expected_attrs=None):
    return IMPL.task_get_all(
        context, marker=marker, limit=limit, sort_keys=sort_keys,
        sort_dirs=sort_dirs, filters=filters, offset=offset,
        expected_attrs=expected_attrs)


def task_get_count(context, filters):
    return IMPL.task_get_count(context, filters=filters)


def task_update(context, task_id, values):
    return IMPL.task_update(context, task_id, values)


###############


def resource_exists(context, model, resource_id):
    return IMPL.resource_exists(context, model, resource_id)


def get_by_id(context, model, id, *args, **kwargs):
    return IMPL.get_by_id(context, model, id, *args, **kwargs)


class Condition(object):
    """Class for normal condition values for conditional_update."""

    def __init__(self, value, field=None):
        self.value = value
        # Field is optional and can be passed when getting the filter
        self.field = field

    def get_filter(self, model, field=None):
        return IMPL.condition_db_filter(model, self._get_field(field),
                                        self.value)

    def _get_field(self, field=None):
        # We must have a defined field on initialization or when called
        field = field or self.field
        if not field:
            raise ValueError(_('Condition has no field.'))
        return field

###################


class Not(Condition):
    """Class for negated condition values for conditional_update.

    By default NULL values will be treated like Python treats None instead of
    how SQL treats it.

    So for example when values are (1, 2) it will evaluate to True when we have
    value 3 or NULL, instead of only with 3 like SQL does.
    """

    def __init__(self, value, field=None, auto_none=True):
        super(Not, self).__init__(value, field)
        self.auto_none = auto_none

    def get_filter(self, model, field=None):
        # If implementation has a specific method use it
        if hasattr(IMPL, 'condition_not_db_filter'):
            return IMPL.condition_not_db_filter(model, self._get_field(field),
                                                self.value, self.auto_none)

        # Otherwise non negated object must adming ~ operator for not
        return ~super(Not, self).get_filter(model, field)


class Case(object):
    """Class for conditional value selection for conditional_update."""

    def __init__(self, whens, value=None, else_=None):
        self.whens = whens
        self.value = value
        self.else_ = else_


def is_orm_value(obj):
    """Check if object is an ORM field."""
    return IMPL.is_orm_value(obj)


def conditional_update(context, model, values, expected_values, filters=(),
                       include_deleted='no', project_only=False, order=None):
    """Compare-and-swap conditional update.

    Update will only occur in the DB if conditions are met.

    We have 4 different condition types we can use in expected_values:
     - Equality:  {'status': 'available'}
     - Inequality: {'status': vol_obj.Not('deleting')}
     - In range: {'status': ['available', 'error']
     - Not in range: {'status': vol_obj.Not(['in-use', 'attaching'])

    Method accepts additional filters, which are basically anything that can be
    passed to a sqlalchemy query's filter method, for example:

    .. code-block:: python

     [~sql.exists().where(models.Volume.id == models.Snapshot.volume_id)]

    We can select values based on conditions using Case objects in the 'values'
    argument. For example:

    .. code-block:: python

     has_snapshot_filter = sql.exists().where(
         models.Snapshot.volume_id == models.Volume.id)
     case_values = db.Case([(has_snapshot_filter, 'has-snapshot')],
                           else_='no-snapshot')
     db.conditional_update(context, models.Volume, {'status': case_values},
                           {'status': 'available'})

    And we can use DB fields for example to store previous status in the
    corresponding field even though we don't know which value is in the db from
    those we allowed:

    .. code-block:: python

     db.conditional_update(context, models.Volume,
                           {'status': 'deleting',
                            'previous_status': models.Volume.status},
                           {'status': ('available', 'error')})

    :param values: Dictionary of key-values to update in the DB.
    :param expected_values: Dictionary of conditions that must be met for the
                            update to be executed.
    :param filters: Iterable with additional filters.
    :param include_deleted: Should the update include deleted items, this is
                            equivalent to read_deleted.
    :param project_only: Should the query be limited to context's project.
    :param order: Specific order of fields in which to update the values
    :returns: Number of db rows that were updated.
    """
    return IMPL.conditional_update(context, model, values, expected_values,
                                   filters, include_deleted, project_only,
                                   order)
