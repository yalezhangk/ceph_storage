import socket

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import netutils


CONF = cfg.CONF
logging.register_options(CONF)

core_opts = [
    cfg.StrOpt('state_path',
               default='/var/lib/cinder',
               help="Top-level directory for maintaining cinder's state"), ]

global_opts = [
    cfg.HostAddressOpt('my_ip',
                       sample_default='<HOST_IP_ADDRESS>',
                       default=netutils.get_my_ipv4(),
                       help='IP address of this host'),
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
               help='Websocket port'),
    cfg.IntOpt('agent_port',
               min=0,
               default=2083,
               help='Websocket port'),
    cfg.HostAddressOpt('hostname',
                       sample_default='localhost',
                       default=socket.gethostname(),
                       help='Name of this node.  This can be an opaque '
                            'identifier. It is not necessarily a host name, '
                            'FQDN, or IP address.'),
]

CONF.register_cli_opts(core_opts)
CONF.register_opts(core_opts)
CONF.register_opts(global_opts)
