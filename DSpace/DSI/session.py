#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import logging
import time
from urllib.parse import parse_qs
from urllib.parse import urlparse

import redis
from redis import sentinel

from DSpace.common.config import CONF

logger = logging.getLogger(__name__)

SESSION = {}


class Session(object):
    def __init__(self, handler):
        self.handler = handler
        self.random_index_str = None

    def __get_random_str(self):
        md = hashlib.md5()
        md.update(bytes(str(time.time()) + ' | dspace-secret',
                        encoding='utf-8'))
        return md.hexdigest()

    def __setitem__(self, key, value):
        if not self.random_index_str:
            random_index_str = self.handler.get_secure_cookie("__sson__", None)
            if not random_index_str:
                self.random_index_str = self.__get_random_str()
                SESSION[self.random_index_str] = {}
            else:
                if random_index_str not in SESSION.keys():
                    self.random_index_str = self.__get_random_str()
                    SESSION[self.random_index_str] = {}
                else:
                    self.random_index_str = random_index_str

        SESSION[self.random_index_str][key] = value

        self.handler.set_secure_cookie('__sson__', self.random_index_str)

    def __getitem__(self, key):
        self.random_index_str = self.handler.get_secure_cookie(
            '__sson__', None)
        if not self.random_index_str:
            return None

        else:
            self.random_index_str = str(self.random_index_str,
                                        encoding="utf-8")
            current_user = SESSION.get(self.random_index_str, None)
            if not current_user:
                return None
            else:
                return current_user.get(key, None)


class RedisSession(Session):
    def __init__(self, handler):
        self.handler = handler
        self.random_index_str = None
        self.client = self.get_client(CONF.session_url)

    def get_client(self, url):
        kwargs = {}
        option = urlparse(CONF.session_url)

        kwargs['host'] = option.hostname
        kwargs['port'] = option.port
        kwargs['password'] = option.password

        query = parse_qs(option.query)

        socket_timeout = query.get("socket_timeout")

        if socket_timeout:
            kwargs['socket_timeout'] = int(socket_timeout[0])
        else:
            kwargs['socket_timeout'] = None

        if 'sentinel' in query:
            sentinel_name = query.get('sentinel')[0]
            sentinel_hosts = [
                tuple(fallback.split(':'))
                for fallback in query.get('sentinel_fallback', [])
            ]
            sentinel_hosts.insert(0, (kwargs['host'], kwargs['port']))
            sentinel_server = sentinel.Sentinel(
                sentinel_hosts,
                socket_timeout=kwargs['socket_timeout'])
            master_client = sentinel_server.master_for(sentinel_name, **kwargs)
            return master_client
        return redis.StrictRedis(**kwargs)

    def __get_random_str(self):
        md = hashlib.md5()
        md.update(bytes(str(time.time()) + ' | dspace-secret',
                        encoding='utf-8'))
        return md.hexdigest()

    def __setitem__(self, key, value):
        if not self.random_index_str:
            random_index_str = self.handler.get_secure_cookie("__sson__", None)
            if not random_index_str:
                self.random_index_str = self.__get_random_str()
                self.client.hset(self.random_index_str, "sessoin", "True")
            else:
                if self.client.hget(random_index_str, "sessoin") != "True":
                    self.random_index_str = self.__get_random_str()
                else:
                    self.random_index_str = random_index_str

        logger.debug("Session(%s) set(%s) value(%s)",
                     self.random_index_str, key, value)
        self.client.hset(self.random_index_str, key, value)

        self.handler.set_secure_cookie('__sson__', self.random_index_str)

    def __getitem__(self, key):
        self.random_index_str = self.handler.get_secure_cookie(
            '__sson__', None)
        if not self.random_index_str:
            return None

        else:
            self.random_index_str = str(self.random_index_str,
                                        encoding="utf-8")
            value = self.client.hget(self.random_index_str, key)
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            logger.debug("Session(%s) get(%s) value(%s)",
                         self.random_index_str, key, value)
            return value


def get_session():
    if CONF.session_url:
        return RedisSession
    else:
        return Session
