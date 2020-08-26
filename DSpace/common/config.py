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
    cfg.IntOpt('ssh_port',
               min=0,
               default=22,
               help='SSH port'),
    cfg.BoolOpt('check_origin',
                default=True,
                help='Check Origin'),
    cfg.StrOpt('socket_file',
               default='/var/lib/dspace/dsa.asok',
               help='unix domain socket file'),
    cfg.StrOpt('ceph_confs_path',
               default='/etc/dspace/ceph_configs.json',
               help='revisable ceph confs'),
    cfg.StrOpt('dsa_lib_dir',
               default='/var/lib/dspace',
               help='dsa lib dir'),
    cfg.IntOpt('node_id',
               default=None,
               help='Node ID'),
    cfg.StrOpt('cookie_secret',
               default="_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
               help="Cookie secret"),
    cfg.StrOpt('api_prefix',
               default="/api",
               help="API prefix"),
    cfg.IntOpt('task_workers',
               default=200,
               help='Task worker number.'),
    cfg.IntOpt('taskflow_max_workers',
               default=200,
               help='Taskflow max worker number.'),
    cfg.StrOpt('host_prefix',
               default="/host",
               help="Host prefix"),
    cfg.StrOpt('session_url',
               default=None,
               help="Session url"),
    cfg.IntOpt('rgw_min_port',
               default=1,
               help='The minimum port number for rgw.'),
    cfg.IntOpt('rgw_max_port',
               default=65535,
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
    cfg.BoolOpt('heartbeat_check',
                default=True,
                help='heartbeat check'),
    cfg.BoolOpt('osd_heartbeat_check',
                default=True,
                help='OSD heartbeat check'),
    cfg.BoolOpt('service_auto_restart',
                default=True,
                help='Service will be restarted if dsm mark it to down'),
    cfg.IntOpt('service_heartbeat_interval',
               default=5,
               help='The interval to check services status in dsa'),
    cfg.IntOpt('ceph_mon_check_interval',
               default=3,
               help='The interval to check ceph mon status'),
    cfg.StrOpt('time_zone',
               default="Asia/Shanghai",
               help='Local time zone'),
    cfg.StrOpt('os_distro',
               default=None,
               help="Current OS Distro"),
    cfg.IntOpt('alert_rule_check_interval',
               default=60,
               help='Alert rule check interval'),
    cfg.IntOpt('alert_rule_average_time',
               default=60,
               help='Alert rule average time'),
    cfg.StrOpt('auth_backend',
               default="DBAuth",
               help='auth_backend'),
    cfg.IntOpt('rados_timeout',
               default=30,
               help='Ceph Rados client timeout'),
    cfg.IntOpt('collect_metrics_time',
               default=15,
               help='DSM: MetricsHandler collect metrics time interval'),
    cfg.BoolOpt('package_ignore',
                default=False,
                help='is or not package_ignore'),
    cfg.IntOpt('erasure_default_pg_num',
               default=32,
               help='DSM: Erasure pool default pg num'),
    cfg.ListOpt('support_raid_models',
                default=['ServeRAID M5210', 'ServeRAID M5110e'],
                help='DSA: Support raid models'),
    cfg.StrOpt('disk_blacklist',
               default="^sr|fd|dm",
               help='Disk blacklist'),
    cfg.ListOpt('disk_bus_blacklist',
                default=['usb'],
                help='DSA: Unsupported dev bus'),
    cfg.StrOpt('ssh_user',
               default="root",
               help='User to establish ssh connection'),
    cfg.StrOpt('ssh_password',
               default="",
               help='Passowrd of ssh user'),
    cfg.StrOpt('ssh_private_key',
               default='',
               help='Private key of ssh user'),
    cfg.StrOpt('sudo_prefix',
               default="sudo",
               help='Prefix to run command with root permission'),
]

etcd_opts = [
    cfg.HostAddressOpt('host',
                       sample_default='<HOST_IP_ADDRESS>',
                       default=netutils.get_my_ipv4(),
                       help='ETCD IP address'),
    cfg.IntOpt('port',
               default=2379,
               help='The port or etcd.'),
]

CONF.register_cli_opts(core_opts)
CONF.register_opts(core_opts)
CONF.register_opts(global_opts)
CONF.register_opts(etcd_opts, group='etcd')

CONF.logging_user_identity_format = '%(user)s %(cluster_id)s'
