#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os


logger = logging.getLogger(__name__)


class DiskTool(object):
    host = None

    def __init__(self, host=None):
        self.host = host

    def _wapper(self, path):
        return os.path.join(self.host, path)

    def all(self):
        res = {}
        path = self._wapper("/sys/class/block/")
        for block in os.listdir(path):
            if block[-1].isdigit():
                continue
            dirs = os.listdir(os.path.join(path, block))
            if 'stat' not in dirs:
                continue
            res[block] = {
                "partitions": {},
                "size": open(os.path.join(path, block, 'size')).read().strip(),
                "rotational": open(
                    os.path.join(path, block, 'queue', 'rotational')
                ).read().strip()
            }
            for partition in dirs:
                if not partition.startswith(block):
                    continue
                res[block]["partitions"][partition] = {
                    "size": open(
                        os.path.join(path, block, partition, 'size')
                    ).read().strip(),
                }
        return res

    def partition_create(self, disk):
        pass

    def partition_clear(self, disk):
        pass


if __name__ == '__main__':
    t = DiskTool()
    print(t.all())
