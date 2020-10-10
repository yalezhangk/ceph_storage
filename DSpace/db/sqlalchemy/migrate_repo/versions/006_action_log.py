from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects.mysql import DATETIME


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    action_logs = Table(
        'action_logs', meta,
        autoload=True
    )
    # 更新字段属性
    if hasattr(action_logs.columns, 'begin_time'):
        begin_time = getattr(action_logs.columns, 'begin_time')
        begin_time.alter(name='begin_time', type=DATETIME(fsp=3))
    if hasattr(action_logs.columns, 'finish_time'):
        finish_time = getattr(action_logs.columns, 'finish_time')
        finish_time.alter(name='finish_time', type=DATETIME(fsp=3))
