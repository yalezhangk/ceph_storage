import datetime

from oslo_config import cfg
from sqlalchemy import Boolean, Column, DateTime, Integer
from sqlalchemy import MetaData, String, Table


# Get default values via config.  The defaults will either
# come from the default values set in the quota option
# configuration or via stor.conf if the user has configured
# default values for quotas there.
CONF = cfg.CONF

CLASS_NAME = 'default'
CREATED_AT = datetime.datetime.now()  # noqa


def define_tables(meta):

    volumes = Table(
        'volumes', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', String(36), primary_key=True, nullable=False),
        Column('user_id', String(255)),
        Column('project_id', String(255)),
        Column('size', Integer),
        Column('status', String(255)),
        Column('display_name', String(255)),
        Column('display_description', String(255)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    return [volumes]


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # create all tables
    # Take care on create order for those with FK dependencies
    tables = define_tables(meta)

    for table in tables:
        table.create()
