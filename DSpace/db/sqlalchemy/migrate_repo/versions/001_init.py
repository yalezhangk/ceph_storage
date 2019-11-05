import datetime

from oslo_config import cfg
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import dialects

# Get default values via config.  The defaults will either
# come from the default values set in the quota option
# configuration or via stor.conf if the user has configured
# default values for quotas there.
CONF = cfg.CONF

CLASS_NAME = 'default'
CREATED_AT = datetime.datetime.now()  # noqa


def InetSmall():
    return String(length=39).with_variant(
        dialects.postgresql.INET(), 'postgresql')


def define_tables(meta):

    clusters = Table(
        'clusters', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', String(36), primary_key=True, nullable=False),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume = Table(
        "volumes", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('volume_name', String(64)),
        Column('size', Integer),
        Column('used', Integer),
        Column('is_link_clone', Boolean, default=False),
        Column('snapshot_num', Integer),
        Column('status', String(64)),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        Column('volume_access_path_id', Integer,
               ForeignKey('volume_access_paths.id')),
        Column('volume_client_group_id', Integer,
               ForeignKey('volume_client_groups.id')),
        Column('pool_id', Integer,
               ForeignKey('pools.id')),
        Column('snapshot_id', Integer),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_snapshot = Table(
        "volume_snapshots", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('uuid', String(36)),
        Column('display_name', String(255)),
        Column('status', String(64)),
        Column('is_protect', Boolean, default=True),
        Column('display_description', String(255)),
        Column('volume_id', Integer,
               ForeignKey('volumes.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    rpc_services = Table(
        "rpc_services", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('service_name', String(36)),
        Column('hostname', String(36)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        Column('endpoint', String(255)),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    datacenter = Table(
        "datacenters", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    rack = Table(
        "racks", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(255)),
        Column('datacenter_id', Integer, ForeignKey('datacenters.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    node = Table(
        "nodes", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('hostname', String(255)),
        Column('ip_address', String(32)),
        Column('object_gateway_ip_address', InetSmall()),
        Column('block_gateway_ip_address', InetSmall()),
        Column('file_gateway_ip_address', InetSmall()),
        Column('storage_cluster_ip_address', InetSmall()),
        Column('storage_public_ip_address', InetSmall()),
        Column('password', String(32)),
        Column('status', String(255)),
        Column('role_admin', Boolean),
        Column('role_monitor', Boolean),
        Column('role_storage', Boolean),
        Column('role_block_gateway', Boolean),
        Column('role_object_gateway', Boolean),
        Column('role_file_gateway', Boolean),
        Column('vendor', String(255)),
        Column('model', String(255)),
        Column('cpu_num', String(255)),
        Column('cpu_model', String(255)),
        Column('cpu_core_num', String(255)),
        Column('mem_size', BigInteger),
        Column('sys_type', String(255)),
        Column('sys_version', String(255)),
        Column('rack_id', Integer, ForeignKey('racks.id')),
        Column('time_diff', BigInteger),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_access_path_gateway = Table(
        'volume_access_path_gateways', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('volume_access_path_id', Integer,
               ForeignKey("volume_access_paths.id")),
        Column('volume_gateway_id', Integer,
               ForeignKey("volume_gateways.id")),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_access_path = Table(
        "volume_access_paths", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('iqn', String(80)),
        Column('name', String(32)),
        Column('status', String(32)),
        Column('type', String(32)),
        Column('chap_enable', Boolean),
        Column('chap_username', String(32)),
        Column('chap_password', String(32)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_gateway = Table(
        "volume_gateways", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_client_group = Table(
        "volume_client_groups", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(32)),
        Column('type', String(32)),
        Column('chap_enable', Boolean),
        Column('chap_username', String(32)),
        Column('chap_password', String(32)),
        Column('volume_access_path_id', Integer,
               ForeignKey('volume_access_paths.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    volume_client = Table(
        "volume_clients", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('client_type', String(32)),
        Column('iqn', String(80)),
        Column('volume_client_group_id', Integer,
               ForeignKey('volume_client_groups.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    pools = Table(
        'pools',
        meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean, nullable=False),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('display_name', String(32), nullable=True),
        Column('pool_id', Integer, nullable=True),
        Column('pool_name', String(64), nullable=True),
        Column('type', String(32), nullable=True),
        Column('role', String(32), nullable=True),
        Column('status', String(32), nullable=True),
        Column('data_chunk_num', Integer, nullable=True),
        Column('coding_chunk_num', Integer, nullable=True),
        Column('size', BigInteger, nullable=True),
        Column('used', BigInteger, nullable=True),
        Column('osd_num', Integer, nullable=True),
        Column('speed_type', String(32), nullable=True),
        Column('replicate_size', Integer, nullable=True),
        Column('failure_domain_type', String(32), nullable=False),
        Column('crush_rule_id', Integer, ForeignKey('crush_rules.id')),
        Column('cluster_id', ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    osds = Table(
        'osds',
        meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean, nullable=False),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('osd_id', String(32), nullable=True),
        Column('size', BigInteger, nullable=True),
        Column('used', BigInteger, nullable=True),
        Column('status', String(32), nullable=True),
        Column('type', String(32), nullable=True),
        Column('disk_type', String(32), nullable=True),
        Column('fsid', String(36), nullable=True),
        Column('mem_read_cache', BigInteger, nullable=True),
        Column('crush_rule_id', Integer, ForeignKey('crush_rules.id')),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('disk_id', Integer, ForeignKey('disks.id')),
        Column('cache_partition_id', Integer,
               ForeignKey('disk_partitions.id')),
        Column('db_partition_id', Integer, ForeignKey('disk_partitions.id')),
        Column('wal_partition_id', Integer,
               ForeignKey('disk_partitions.id')),
        Column('journal_partition_id', Integer,
               ForeignKey('disk_partitions.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    disks = Table(
        "disks", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(32)),
        Column('status', String(32)),
        Column('type', String(32)),
        Column('disk_size', BigInteger),
        Column('rotate_speed', Integer),
        Column('slot', String(32)),
        Column('model', String(32)),
        Column('vendor', String(32)),
        Column('support_led', Boolean, default=False),
        Column('led', String(3), default='off'),
        Column('has_patrol', Boolean, default=False),
        Column('patrol_data', String(2048)),
        Column('residual_life', Integer),
        Column('role', String(32), default='data', index=True),
        Column('partition_num', Integer),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    disk_partitions = Table(
        "disk_partitions", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(32)),
        Column('size', BigInteger),
        Column('status', String(32)),
        Column('type', String(32)),
        Column('role', String(32), default='cache', index=True),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('disk_id', Integer, ForeignKey('disks.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    sysconf = Table(
        "sys_configs", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('key', String(255)),
        Column('value', String(255)),
        Column('value_type', String(36)),
        Column('display_description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    ceph_config = Table(
        "ceph_configs", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('key', String(255)),
        Column('group', String(255)),
        Column('value', String(255)),
        Column('value_type', String(36)),
        Column('display_description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    license_files = Table(
        'license_files', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('content', String(2048)),
        Column('status', String(32)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    log_files = Table(
        'log_files', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('service_type', String(32)),
        Column('directory', String(255)),
        Column('filename', String(64)),
        Column('filesize', Integer),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    alert_group_relate_rule = Table(
        'alert_group_relate_rule', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('alert_groups_id', Integer, ForeignKey("alert_groups.id")),
        Column('alert_rules_id', Integer, ForeignKey("alert_rules.id")),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    alert_group_relate_email = Table(
        'alert_group_relate_email', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('alert_groups_id', Integer, ForeignKey("alert_groups.id")),
        Column('email_groups_id', Integer, ForeignKey("email_groups.id")),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    alert_groups = Table(
        'alert_groups', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('name', String(64)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    alert_rules = Table(
        'alert_rules', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('resource_type', String(32)),
        Column('type', String(64)),
        Column('trigger_value', String(64)),
        Column('level', String(64)),
        Column('trigger_period', String(64)),
        Column('enabled', Boolean, default=False),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    email_groups = Table(
        'email_groups', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('name', String(64)),
        Column('emails', String(1024)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    alert_logs = Table(
        'alert_logs', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('readed', Boolean, default=False),
        Column('resource_type', String(32)),
        Column('level', String(32)),
        Column('alert_value', String(1024)),
        Column('resource_id', String(32)),
        Column('resource_name', String(64)),
        Column('alert_role_id', Integer, ForeignKey('alert_rules.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    action_logs = Table(
        'action_logs', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('begin_time', DateTime),
        Column('finish_time', DateTime),
        Column('client_ip', String(64)),
        Column('user_id', String(32)),
        Column('action', String(32)),
        Column('resource_id', String(32)),
        Column('resource_name', String(64)),
        Column('resource_type', String(32)),
        Column('resource_data', Text()),
        Column('status', String(32), default='under way'),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    networks = Table(
        'networks', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('name', String(32)),
        Column('status', String(32)),
        Column('ip_address', String(32)),
        Column('netmask', String(32)),
        Column('mac_address', String(32)),
        Column('type', String(32)),
        Column('speed', String(32)),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    services = Table(
        'services', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('name', String(32)),
        Column('status', String(32)),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    crush_rules = Table(
        'crush_rules', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('rule_name', String(32)),
        Column('rule_id', Integer),
        Column('type', String(32)),
        Column('content', Text()),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    users = Table(
        'users', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True),
        Column('name', String(64)),
        Column('password', String(255)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    return [clusters, crush_rules, pools, volume_access_path,
            volume_client_group, volume, volume_snapshot, datacenter,
            rack, node, rpc_services, disks, disk_partitions, volume_gateway,
            volume_access_path_gateway, volume_client, osds,
            sysconf, ceph_config, license_files, log_files, alert_rules,
            email_groups, alert_groups, alert_group_relate_rule,
            alert_group_relate_email, alert_logs, action_logs,
            networks, services, users]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
