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
        Column('gateway_ip_address', String(32)),
        Column('storage_cluster_ip_address', String(32)),
        Column('storage_public_ip_address', String(32)),
        Column('password', String(32)),
        Column('status', String(255)),
        Column('role_base', Boolean),
        Column('role_admin', Boolean),
        Column('role_monitor', Boolean),
        Column('role_storage', Boolean),
        Column('role_block_gateway', Boolean),
        Column('role_object_gateway', Boolean),
        Column('vendor', String(255)),
        Column('model', String(255)),
        Column('cpu_num', String(255)),
        Column('cpu_model', String(255)),
        Column('cpu_core_num', String(255)),
        Column('mem_num', BigInteger),
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

    return [clusters, volume, volume_snapshot, rpc_services, datacenter,
            rack, node, sysconf, volume_access_path, volume_ap_gateway,
            volume_client_group, volume_client]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
