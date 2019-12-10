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
    cfg.StrOpt('socket_file',
               default='/var/run/dspace/dsa.asok',
               help='unix domain socket file'),
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
    cfg.StrOpt('session_url',
               default=None,
               help="Session url"),
    cfg.IntOpt('rgw_min_port',
               default=7480,
               help='The minimum port number for rgw.'),
    cfg.IntOpt('rgw_max_port',
               default=65500,
               help='The maximum port number for rgw.'),
    cfg.IntOpt('slow_request_get_time_interval',
               default=10,
               help='Gets the time interval for slow requests'),
    cfg.IntOpt('osd_check_interval',
               default=5,
               help='The interval of osd status check'),
    cfg.IntOpt('dsa_check_interval',
               default=10,
               help='The interval of dsa status check'),
    cfg.IntOpt('node_check_interval',
               default=5,
               help='The interval of node status check'),
    cfg.IntOpt('service_max_interval',
               default=30,
               help='The max interval of services to mark down'),
    cfg.IntOpt('service_heartbeat_interval',
               default=5,
               help='The interval to check services status in dsa'),
]

CONF.register_cli_opts(core_opts)
CONF.register_opts(core_opts)
CONF.register_opts(global_opts)

CONF.logging_user_identity_format = '%(user)s %(cluster_id)s'
