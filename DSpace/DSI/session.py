#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import logging
import time

logger = logging.getLogger(__name__)

SESSION = {}


class Session:
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
                self.handler.set_secure_cookie('__sson__',
                                               self.random_index_str)
                SESSION[self.random_index_str] = {}
            else:
                if self.random_index_str not in SESSION.keys():
                    self.random_index_str = self.__get_random_str()
                    SESSION[self.random_index_str] = {}

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
