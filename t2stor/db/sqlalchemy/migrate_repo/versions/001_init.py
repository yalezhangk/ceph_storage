import datetime

from oslo_config import cfg
from sqlalchemy import Boolean, Column, DateTime, Index, Integer
from sqlalchemy import ForeignKey, BigInteger
from sqlalchemy import MetaData, String, Table


# Get default values via config.  The defaults will either
# come from the default values set in the quota option
# configuration or via stor.conf if the user has configured
# default values for quotas there.
CONF = cfg.CONF

CLASS_NAME = 'default'
CREATED_AT = datetime.datetime.now()  # noqa


def define_tables(meta):

    clusters = Table(
        'clusters', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', String(36), primary_key=True, nullable=False),
        Column('table_id', String(36)),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
        Index('table_id_idx', 'table_id', unique=True),
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
        Column('size', Integer),
        Column('used', Integer),
        Column('snapshot_num', Integer),
        Column('status', String(255)),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        Column('access_path_id', Integer,
               ForeignKey('volume_access_paths.id')),
        Column('volume_client_group_id', Integer,
               ForeignKey('volume_client_groups.id')),
        Column('pool_id', Integer,
               ForeignKey('pools.id')),
        Column('snapshot_id', Integer,
               ForeignKey('snap')),
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
        Column('status', String(255)),
        Column('is_protect', Boolean),
        Column('size', Integer),
        Column('used', Integer),
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
        Column('object_gateway_ip_address', String(32)),
        Column('block_gateway_ip_address', String(32)),
        Column('file_gateway_ip_address', String(32)),
        Column('storage_cluster_ip_address', String(32)),
        Column('storage_public_ip_address', String(32)),
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

    sysconf = Table(
        "sysconfs", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('service_id', String(36)),
        Column('key', String(255)),
        Column('value', String(255)),
        Column('value_type', String(36)),
        Column('display_description', String(255)),
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

    volume_ap_gateway = Table(
        "volume_ap_gateways", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('iqn', String(80)),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('volume_access_path_id', Integer,
               ForeignKey('volume_access_paths.id')),
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

    osd_pools = Table(
        'osd_pools',
        meta,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('osd_id', Integer, ForeignKey("osds.id")),
        Column('pool_id', Integer, ForeignKey("pools.id")),
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
        Column('crush_rule', String(64), nullable=True),
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
        Column('name', String(32), nullable=True),
        Column('size', BigInteger, nullable=True),
        Column('used', BigInteger, nullable=True),
        Column('status', String(32), nullable=True),
        Column('type', String(32), nullable=True),
        Column('role', String(32), nullable=True),
        Column('fsid', String(36), nullable=True),
        Column('mem_read_cache', BigInteger, nullable=True),
        Column('node_id', Integer, ForeignKey('nodes.id')),
        Column('disk_id', Integer, ForeignKey('disks.id')),
        Column('cache_partition_id', Integer,
               ForeignKey('disk_partitions.id')),
        Column('db_partition_id', Integer, ForeignKey('disk_partitions.id')),
        Column('wal_partition_id', Integer, ForeignKey('disk_partitions.id')),
        Column('journal_partition_id', Integer,
               ForeignKey('disk_partitions.id')),
        Column('pool_id', Integer, ForeignKey('pools.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    return [clusters, volume, volume_snapshot, rpc_services, datacenter,
            rack, node, sysconf, volume_access_path, volume_ap_gateway,
            volume_client_group, volume_client, pools, osds, osd_pools]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
