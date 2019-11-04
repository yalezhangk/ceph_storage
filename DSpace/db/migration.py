import os
import threading

from oslo_config import cfg
from oslo_db import options
from stevedore import driver

from DSpace.db.sqlalchemy import api as db_api

INIT_VERSION = 0

_IMPL = None
_LOCK = threading.Lock()

options.set_defaults(cfg.CONF)

MIGRATE_REPO_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'sqlalchemy',
    'migrate_repo',
)


def get_backend():
    global _IMPL
    if _IMPL is None:
        with _LOCK:
            if _IMPL is None:
                _IMPL = driver.DriverManager(
                    "dspace.database.migration_backend",
                    cfg.CONF.database.backend).driver
    return _IMPL


def db_sync(version=None, init_version=INIT_VERSION, engine=None):
    """Migrate the database to `version` or the most recent version."""

    if engine is None:
        engine = db_api.get_engine()
    return get_backend().db_sync(engine=engine,
                                 abs_path=MIGRATE_REPO_PATH,
                                 version=version,
                                 init_version=init_version)
