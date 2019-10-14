from oslo_cache import core as cache
from oslo_log import log as logging

from t2stor.common.config import CONF
from t2stor.i18n import _

LOG = logging.getLogger(__name__)

WEEK = 604800


def _warn_if_null_backend():
    if CONF.cache.backend == 'dogpile.cache.null':
        LOG.warning("Cache enabled with backend dogpile.cache.null.")


def get_client(expiration_time=0):
    """Used to get a caching client."""
    # If the operator has [cache]/enabled flag on then we let oslo_cache
    # configure the region from configuration settings.
    if CONF.cache.enabled:
        _warn_if_null_backend()
        return CacheClient(
            _get_default_cache_region(expiration_time=expiration_time))
    # If [cache]/enabled flag is off, we use the dictionary backend
    return CacheClient(
        _get_custom_cache_region(expiration_time=expiration_time,
                                 backend='oslo_cache.dict'))


def _get_default_cache_region(expiration_time):
    region = cache.create_region()
    if expiration_time != 0:
        CONF.cache.expiration_time = expiration_time
    cache.configure_cache_region(CONF, region)
    return region


def _get_custom_cache_region(expiration_time=WEEK,
                             backend=None,
                             url=None):
    """Create instance of oslo_cache client.

    For backends you can pass specific parameters by kwargs.
    For 'dogpile.cache.memcached' backend 'url' parameter must be specified.

    :param backend: backend name
    :param expiration_time: interval in seconds to indicate maximum
        time-to-live value for each key
    :param url: memcached url(s)
    """

    region = cache.create_region()
    region_params = {}
    if expiration_time != 0:
        region_params['expiration_time'] = expiration_time

    if backend == 'oslo_cache.dict':
        region_params['arguments'] = {'expiration_time': expiration_time}
    elif backend == 'dogpile.cache.memcached':
        region_params['arguments'] = {'url': url}
    else:
        raise RuntimeError(_('old style configuration can use '
                             'only dictionary or memcached backends'))

    region.configure(backend, **region_params)
    return region


class CacheClient(object):
    """Replicates a tiny subset of memcached client interface."""

    def __init__(self, region):
        self.region = region

    def get(self, key):
        value = self.region.get(key)
        if value == cache.NO_VALUE:
            return None
        return value

    def get_or_create(self, key, creator):
        return self.region.get_or_create(key, creator)

    def set(self, key, value):
        return self.region.set(key, value)

    def add(self, key, value):
        return self.region.get_or_create(key, lambda: value)

    def delete(self, key):
        return self.region.delete(key)

    def get_multi(self, keys):
        values = self.region.get_multi(keys)
        return [None if value is cache.NO_VALUE else value for value in
                values]

    def delete_multi(self, keys):
        return self.region.delete_multi(keys)
