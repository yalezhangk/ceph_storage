#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import random
import socket
import struct
import time

import netaddr
import retrying
import six
from dateutil import tz

logger = logging.getLogger(__name__)
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


def get_shortened_ipv6(address):
    addr = netaddr.IPAddress(address, version=6)
    return str(addr.ipv6())


def get_shortened_ipv6_cidr(address):
    net = netaddr.IPNetwork(address, version=6)
    return str(net.cidr)


def logical_xor(v1, v2):
    return bool(v1) ^ bool(v2)


def retry(exceptions, interval=1, retries=3, backoff_rate=2,
          wait_random=False):

    def _retry_on_exception(e):
        return isinstance(e, exceptions)

    def _backoff_sleep(previous_attempt_number, delay_since_first_attempt_ms):
        exp = backoff_rate ** previous_attempt_number
        wait_for = interval * exp

        if wait_random:
            random.seed()
            wait_val = random.randrange(interval * 1000.0, wait_for * 1000.0)
        else:
            wait_val = wait_for * 1000.0

        logger.debug("Sleeping for %s seconds", (wait_val / 1000.0))

        return wait_val

    def _print_stop(previous_attempt_number, delay_since_first_attempt_ms):
        delay_since_first_attempt = delay_since_first_attempt_ms / 1000.0
        logger.debug("Failed attempt %s", previous_attempt_number)
        logger.debug("Have been at this for %s seconds",
                     delay_since_first_attempt)
        return previous_attempt_number == retries

    if retries < 1:
        raise ValueError('Retries must be greater than or '
                         'equal to 1 (received: %s). ' % retries)

    def _decorator(f):

        @six.wraps(f)
        def _wrapper(*args, **kwargs):
            r = retrying.Retrying(retry_on_exception=_retry_on_exception,
                                  wait_func=_backoff_sleep,
                                  stop_func=_print_stop)
            return r.call(f, *args, **kwargs)

        return _wrapper

    return _decorator


def versiontuple(v):
    filled = []
    for point in v.split("."):
        filled.append(point.zfill(8))
    return tuple(filled)


def utc_to_local(source, zone):
    from_zone = tz.tzutc()
    to_zone = tz.gettz(zone)

    # Tell the datetime object that it's in UTC time zone since
    # datetime objects are 'naive' by default
    source = source.replace(tzinfo=from_zone)

    # Convert time zone
    return source.astimezone(to_zone)


def cidr2network(cidr):
    items = cidr.split('/')
    if len(items) != 2:
        return None
    address, netmask_length = items
    address_bin = struct.unpack('!L', socket.inet_aton(address))[0]
    netmask_bin = (1 << 32) - (1 << 32 >> int(netmask_length))
    network = socket.inet_ntoa(
        struct.pack('!L', address_bin & netmask_bin)
    )
    return network


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


def no_exception(fun):
    @six.wraps(fun)
    def _wrapper(*args, **kwargs):
        try:
            r = fun(*args, **kwargs)
        except Exception as e:
            logger.warning("%s call error: %s", fun, e)
            r = None
        return r
    return _wrapper
