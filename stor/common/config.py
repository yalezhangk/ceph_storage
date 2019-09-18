from oslo_config import cfg
from oslo_log import log as logging


CONF = cfg.CONF
logging.register_options(CONF)

core_opts = [
    cfg.StrOpt('state_path',
               default='/var/lib/cinder',
               help="Top-level directory for maintaining cinder's state"), ]

CONF.register_cli_opts(core_opts)
CONF.register_opts(core_opts)
