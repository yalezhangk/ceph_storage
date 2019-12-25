from os import path

CEPH_CONFIG_DIR = "/etc/ceph/"
CEPH_CONFIG_PATH = path.join(CEPH_CONFIG_DIR, "ceph.conf")

SUPPORT_CEPH_VERSION = [{
    "name": "luminous",
    "version": "12.2.10",
    "release": "0"
}]

# cluster configs
# TODO add other configs
cluster_sys_configs = ['chrony_server', 'cluster_name', 'mon_available']

cluster_osd_restart_configs = ['bluestore_cache_size',
                               'bluestore_cache_size_ssd',
                               'bluestore_cache_size_hdd',
                               'osd_crush_update_on_start',
                               'osd_pool_default_type',
                               'backend_type']

cluster_mon_restart_configs = []

cluster_rgw_restart_configs = ['rgw_thread_pool_size']

cluster_rgw_temp_configs = ['debug_rgw']

cluster_osd_temp_configs = ['debug_ms', 'debug_osd', 'debug_bluestore',
                            'debug_rbd', 'debug_rados', 'debug_perfcounter',
                            't2ce_iobypass_size_kb', 't2ce_flush_water_level',
                            't2ce_iobypass_water_level', 't2ce_gc_moving_skip']

cluster_mon_temp_configs = ['debug_ms', 'debug_mon', 'debug_perfcounter',
                            'mon_allow_pool_delete']

cluster_configs = {
    'debug_osd': {'type': 'int', 'default': 5},
    'debug_mon': {'type': 'int', 'default': 5},
    'debug_rgw': {'type': 'int', 'default': 5},
    'osd_pool_default_type': {'type': 'string', 'default': 'replicated'},
    'backend_type': {'type': 'string', 'default': 't2ce'},
    'cluster_network': {'type': 'string', 'default': ''},
    'public_network': {'type': 'string', 'default': ''},
    'mon_initial_members': {'type': 'string', 'default': ''},
    'mon_host': {'type': 'string', 'default': ''},
    'fsid': {'type': 'string', 'default': ''},
    'osd_crush_update_on_start': {'type': 'bool', 'default': False},
    'auth_client_required': {'type': 'string', 'default': 'none'},
    'auth_service_required': {'type': 'string', 'default': 'none'},
    'auth_cluster_required': {'type': 'string', 'default': 'none'},
    'mon_allow_pool_delete': {'type': 'bool', 'default': True},
}

default_cluster_configs = {
    'mon_allow_pool_delete': {'type': 'bool', 'value': True},
    'osd_crush_update_on_start': {'type': 'bool', 'value': False},
    'osd_pool_default_min_size': {'type': 'int', 'value': 1},
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


def get_full_ceph_version(v):
    if not v:
        return False
    version = v.get('version')
    if not version:
        return False
    versions = filter(lambda v: v['version'] == version,
                      SUPPORT_CEPH_VERSION)
    release = v.get('release')
    if not release:
        versions = filter(lambda v: v['release'] == release,
                          versions)
    versions = list(versions)
    if versions:
        return versions[0]
    return False
