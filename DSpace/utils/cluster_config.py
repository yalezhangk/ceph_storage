from os import path

from DSpace.i18n import _

CEPH_CONFIG_DIR = "/etc/ceph/"
CEPH_CONFIG_PATH = path.join(CEPH_CONFIG_DIR, "ceph.conf")
CEPH_LIB_DIR = "/var/lib/ceph/"
CEPH_SYSTEMD_DIRS = [
    "/etc/systemd/system/ceph-mds.target.wants",
    "/etc/systemd/system/ceph-mgr.target.wants",
    "/etc/systemd/system/ceph-radosgw.target.wants",
    "/etc/systemd/system/ceph.target.wants"
]

SUPPORT_CEPH_VERSION = [{
    "name": "luminous",
    "version": "12.2.10",
    "release": "0"
}, {
    "name": "t2stor",
    "version": "12.2.10",
    "release": "507"
}]

PKG_MGR = {
    "Kylin": "apt",
    "Debian": "apt",
    "NeoKylin": "yum",
    "RedHat": "yum"
}

UDEV_DIR = {
    "Kylin": "/lib/udev/rules.d/",
    "Debian": "/lib/udev/rules.d/",
    "NeoKylin": "/usr/lib/udev/rules.d/",
    "RedHat": "/usr/lib/udev/rules.d/"
}

# cluster configs
# TODO add other configs
cluster_sys_configs = ['chrony_server', 'cluster_name', 'mon_available']

cluster_osd_restart_configs = ['bluestore_cache_size',
                               'bluestore_cache_size_ssd',
                               'bluestore_cache_size_hdd',
                               'osd_crush_update_on_start',
                               'osd_pool_default_type',
                               'backend_type']

cluster_mon_restart_configs = ['osd_pool_default_type',
                               'osd_pool_default_size',
                               'osd_pool_default_min_size']

cluster_rgw_restart_configs = ['rgw_thread_pool_size']

cluster_rgw_temp_configs = []

cluster_osd_temp_configs = ['debug_ms', 'debug_osd', 'debug_bluestore',
                            'debug_rbd', 'debug_rados', 'debug_perfcounter',
                            't2ce_iobypass_size_kb', 't2ce_flush_water_level',
                            't2ce_iobypass_water_level', 't2ce_gc_moving_skip']

cluster_mon_temp_configs = ['debug_ms', 'debug_mon', 'debug_perfcounter',
                            'mon_allow_pool_delete']

cluster_configs = {
    'debug_osd': {'type': 'int', 'default': 5},
    'debug_mon': {'type': 'int', 'default': 5},
    'debug_mds': {'type': 'int', 'default': 5},
    'debug_mgr': {'type': 'int', 'default': 5},
    'debug_ms': {'type': 'int', 'default': 5},
    'osd_pool_default_type': {'type': 'string', 'default': 'replicated'},
    'osd_pool_default_size': {'type': 'int', 'default': 1},
    'osd_pool_default_min_size': {'type': 'int', 'default': 1},
}

default_cluster_configs = {
    'mon_allow_pool_delete': {'type': 'bool', 'value': True},
    'osd_crush_update_on_start': {'type': 'bool', 'value': False},
    'osd_pool_default_type': {'type': 'string', 'value': 'replicated'},
    'osd_pool_default_size': {'type': 'int', 'value': 1},
    'osd_pool_default_min_size': {'type': 'int', 'value': 1},
    'backend_type': {'type': 'string', 'value': 'kernel'},
}

auth_none_config = {
    'auth_cluster_required': {'type': 'string', 'value': 'none'},
    'auth_service_required': {'type': 'string', 'value': 'none'},
    'auth_client_required': {'type': 'string', 'value': 'none'},
}

auth_cephx_config = {
    'auth_cluster_required': {'type': 'string', 'value': 'cephx'},
    'auth_service_required': {'type': 'string', 'value': 'cephx'},
    'auth_client_required': {'type': 'string', 'value': 'cephx'},
}

type_translation = {
    "int": _("int"),
    "bool": _("bool"),
    "string": _("string"),
}


def get_full_ceph_version(v):
    if not v:
        return False
    version = v.get('version')
    if not version:
        return False
    versions = filter(lambda v: v['version'] == version,
                      SUPPORT_CEPH_VERSION)
    release = v.get('release')
    if release:
        versions = filter(lambda v: v['release'] == release,
                          versions)
    versions = list(versions)
    if versions:
        return versions[0]
    return False
