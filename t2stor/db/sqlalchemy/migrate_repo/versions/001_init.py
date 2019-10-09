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

    volumes = Table(
        "volumes", meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('user_id', String(255)),
        Column('project_id', String(255)),
        Column('size', Integer),
        Column('status', String(255)),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
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

    return [clusters, volumes, rpc_services, datacenter, rack, node, sysconf]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
