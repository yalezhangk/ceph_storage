from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    snapshot_table = Table(
        'volume_snapshots', meta,
        autoload=True
    )
    size = Column('size', BigInteger)
    if not hasattr(snapshot_table.columns, 'size'):
        snapshot_table.create_column(size)
