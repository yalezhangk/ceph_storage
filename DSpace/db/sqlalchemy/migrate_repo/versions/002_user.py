#!/usr/bin/env python
# -*- coding: utf-8 -*-
from oslo_utils import timeutils
from sqlalchemy import MetaData
from sqlalchemy import Table

from DSpace.utils.security import encrypt_password


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    user_table = Table('users', meta, autoload=True)
    now = timeutils.utcnow().replace(microsecond=123)
    user_data = {
        'name': 'admin',
        'password': encrypt_password('a'),
        'created_at': now,
        'updated_at': now,
        'deleted': False,
    }
    user = user_table.insert()
    user.execute(user_data)
