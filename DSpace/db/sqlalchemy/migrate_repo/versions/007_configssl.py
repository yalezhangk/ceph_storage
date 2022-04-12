#!/usr/bin/env python
# -*- coding: utf-8 -*-


from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    table = Table(
        'configssls', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean, index=True),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(64), index=True),
        Column('crt', Text),
        Column('key', Text),
        Column('not_before', DateTime),
        Column('not_after', DateTime),
        Column('domain_name', String(255), index=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    table.create()
