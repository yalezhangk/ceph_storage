#!/usr/bin/env python
# -*- coding: utf-8 -*-
import abc
import logging

import six

from DSpace.common.config import CONF

logger = logging.getLogger(__name__)


# TODO: Common Registry
class AuthRegistry(object):
    _registry = None
    _auth = None

    def __new__(cls, *args, **kwargs):
        if not cls._registry:
            cls._registry = super(AuthRegistry, cls).__new__(
                cls, *args, **kwargs)
        return cls._registry

    def __init__(self, *args, **kwargs):
        if self._auth is None:
            self._auth = {}

    @classmethod
    def register(cls, tf_cls):
        logger.info("register auth %s", tf_cls)
        registry = cls()
        registry._register_class(tf_cls)
        return tf_cls

    def _register_class(self, cls):
        name = cls.obj_name()
        if name not in self._auth:
            logger.info("auth %s registered", name)
            self._auth[name] = cls

    @property
    def auth_cls(self):
        name = CONF.auth_backend
        return self._auth[name]


@six.add_metaclass(abc.ABCMeta)
class AuthBackend(object):

    @abc.abstractmethod
    def validate(self, handler):
        pass

    @classmethod
    def obj_name(cls):
        """Return the object's name"""
        return cls.__name__

    @classmethod
    def register_url(cls):
        pass


def register_all():
    __import__('DSpace.DSI.auth.tencent')
    __import__('DSpace.DSI.auth.db')
    __import__('DSpace.DSI.auth.hci_sso')
    AuthRegistry().auth_cls.register_url()
