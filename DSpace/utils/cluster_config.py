
# cluster configs
cluster_sys_configs = ['chrony_server', 'cluster_name', 'mon_available']

cluster_osd_restart_configs = ['bluestore_cache_size',
                               'bluestore_cache_size_ssd',
                               'bluestore_cache_size_hdd']

cluster_mon_restart_configs = []

cluster_rgw_restart_configs = ['rgw_thread_pool_size']

cluster_rgw_temp_configs = ['debug_rgw']

cluster_osd_temp_configs = ['debug_ms', 'debug_osd', 'debug_bluestore',
                            'debug_rbd', 'debug_rados', 'debug_perfcounter',
                            't2ce_iobypass_size_kb', 't2ce_flush_water_level',
                            't2ce_iobypass_water_level', 't2ce_gc_moving_skip']

cluster_mon_temp_configs = ['debug_ms', 'debug_mon', 'debug_perfcounter']

cluster_configs = {
    'debug_osd': {'type': 'int', 'default': 5},
    'debug_mon': {'type': 'int', 'default': 5},
    'debug_rgw': {'type': 'int', 'default': 5},
    'osd_pool_default_type': {'type': 'string', 'default': 'replicated'},
}

default_cluster_configs = {
    'mon_allow_pool_delete': {'type': 'bool', 'value': True},
    'backend_type': {'type': 'string', 'value': 'kernel'},
    'auth_cluster_required': {'type': 'string', 'value': 'none'},
    'auth_service_required': {'type': 'string', 'value': 'none'},
    'auth_client_required': {'type': 'string', 'value': 'none'},
    'osd_crush_update_on_start': {'type': 'bool', 'value': False},
}
