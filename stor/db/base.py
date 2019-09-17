from oslo_config import cfg
from oslo_utils import importutils
import six


db_driver_opt = cfg.StrOpt('db_driver',
                           default='stor.db',
                           help='Driver to use for database access')

CONF = cfg.CONF
CONF.register_opt(db_driver_opt)


class Base(object):
    """DB driver is injected in the init method."""

    def __init__(self, db_driver=None):
        # NOTE(mriedem): Without this call, multiple inheritance involving
        # the db Base class does not work correctly.
        super(Base, self).__init__()
        if not db_driver:
            db_driver = CONF.db_driver

        # pylint: disable=C0103
        if isinstance(db_driver, six.string_types):
            self.db = importutils.import_module(db_driver)
        else:
            self.db = db_driver
        self.db.dispose_engine()
