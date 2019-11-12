import socket

from oslo_cache import core as cache
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import netutils

CONF = cfg.CONF
logging.register_options(CONF)
cache.configure(CONF)

core_opts = [
    cfg.StrOpt('state_path',
               default='/var/lib/dspace',
               help="Top-level directory for maintaining DSpace's state"), ]

global_opts = [
    cfg.HostAddressOpt('my_ip',
                       sample_default='<HOST_IP_ADDRESS>',
                       default=netutils.get_my_ipv4(),
                       help='IP address of this host'),
    cfg.HostAddressOpt('admin_ip',
                       sample_default='<HOST_IP_ADDRESS>',
                       default=netutils.get_my_ipv4(),
                       help='IP address of DSM service'),
    cfg.IntOpt('api_port',
               min=0,
               default=2080,
               help='Api port'),
    cfg.IntOpt('websocket_port',
               min=0,
               default=2081,
               help='Websocket port'),
    cfg.IntOpt('admin_port',
               min=0,
               default=2082,
               help='DSM port'),
    cfg.IntOpt('agent_port',
               min=0,
               default=2083,
               help='Websocket port'),
    cfg.BoolOpt('check_origin',
                default=True,
                help='Check Origin'),
    cfg.IntOpt('node_id',
               default=None,
               help='Node ID'),
    cfg.HostAddressOpt('hostname',
                       sample_default='localhost',
                       default=socket.gethostname(),
                       help='Name of this node.  This can be an opaque '
                            'identifier. It is not necessarily a host name, '
                            'FQDN, or IP address.'),
    cfg.StrOpt('cookie_secret',
               default="_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
               help="Cookie secret"),
    cfg.StrOpt('api_prefix',
               default="/api",
               help="API prefix"),
    cfg.IntOpt('task_workers',
               default=50,
               help='Task worker number.'),
    cfg.StrOpt('host_prefix',
               default="/host",
               help="Host prefix"),
]

CONF.register_cli_opts(core_opts)
CONF.register_opts(core_opts)
CONF.register_opts(global_opts)
