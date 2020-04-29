#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
from functools import wraps

from DSpace.objects.fields import PoolType
from DSpace.tools.ceph import EC_POOL_RELATION_RE_POOL as ECP

logger = logging.getLogger(__name__)


def get_file_content(path, default=None, strip=True):
    data = default
    if os.path.exists(path) and os.access(path, os.R_OK):
        try:
            try:
                datafile = open(path)
                data = datafile.read()
                if strip:
                    data = data.strip()
                if len(data) == 0:
                    data = default
            finally:
                datafile.close()
        except Exception:
            pass
    return data


def change_erasure_pool_name(child_name_position=None):
    # child_name_position type must be int
    def _decorator(fun):
        @wraps(fun)
        def _wapper(self, *args, **kwargs):
            # first args must is pool_name
            # pool_type in kwargs
            # child_pool_type in kwargs if child_name_position
            pool_name = args[0]
            pool_type = kwargs.get('pool_type')
            if child_name_position and (child_name_position > (len(args) - 1)):
                raise Exception('tuple index out of range')
            new_args = list(args[1:])
            if pool_type == PoolType.ERASURE:
                pool_name = pool_name + ECP
            new_args.insert(0, pool_name)
            c_pool_type = kwargs.get('child_pool_type')
            if c_pool_type == PoolType.ERASURE:
                # c_pool_name change name
                c_pool_name = args[child_name_position]
                c_pool_name = c_pool_name + ECP
                new_args[child_name_position] = c_pool_name
            return fun(self, *tuple(new_args), **kwargs)
        return _wapper
    return _decorator
