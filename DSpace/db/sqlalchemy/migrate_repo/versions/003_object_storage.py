from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table


def define_tables(meta):
    Table('pools', meta, autoload=True)
    object_policy = Table(
        'object_policies', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(64), index=True),
        Column('description', String(255)),
        Column('default', Boolean, default=False),
        Column('index_pool_id', Integer, ForeignKey('pools.id')),
        Column('data_pool_id', Integer, ForeignKey('pools.id')),
        Column('compression', String(36)),
        Column('index_type', Integer, default=0),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    object_user = Table(
        'object_users', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('uid', String(32)),
        Column('email', String(32)),
        Column('display_name', String(64), index=True),
        Column('status', String(32)),
        Column('suspended', Boolean, default=False),
        Column('op_mask', String(32)),
        Column('max_bucket', BigInteger),
        Column('bucket_quota_max_size', BigInteger),
        Column('bucket_quota_max_objects', BigInteger),
        Column('user_quota_max_size', BigInteger),
        Column('user_quota_max_objects', BigInteger),
        Column('description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    object_access_key = Table(
        'object_access_keys', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('obj_user_id', Integer, ForeignKey('object_users.id')),
        Column('access_key', String(64)),
        Column('secret_key', String(64)),
        Column('type', String(32)),
        Column('description', String(255)),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    object_bucket = Table(
        'object_buckets', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(64), index=True),
        Column('status', String(64)),
        Column('bucket_id', String(64)),
        Column('policy_id', Integer, ForeignKey('object_policies.id')),
        Column('owner_id', Integer, ForeignKey('object_users.id')),
        Column('shards', Integer),
        Column('versioned', Boolean),
        Column('owner_permission', String(32)),
        Column('auth_user_permission', String(32)),
        Column('all_user_permission', String(32)),
        Column('quota_max_size', BigInteger),
        Column('quota_max_objects', BigInteger),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    lifecycle = Table(
        'object_lifecycles', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(64), index=True),
        Column('enabled', Boolean, default=True),
        Column('target', String(128)),
        Column('policy', String(1024)),
        Column('bucket_id', Integer, ForeignKey('object_buckets.id')),
        Column('cluster_id', String(36), ForeignKey('clusters.id')),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    return [object_policy, object_user, object_access_key, object_bucket,
            lifecycle]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
