#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import random

import netaddr
import retrying
import six

logger = logging.getLogger(__name__)


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
