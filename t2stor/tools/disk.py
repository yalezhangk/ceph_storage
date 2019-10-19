#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

from t2stor.tools.base import ToolBase
from t2stor.exception import ProgrammingError


logger = logging.getLogger(__name__)


class DiskTool(ToolBase):
    host_prefix = None

    def __init__(self, *args, host_prefix=None, **kwargs):
        self.host_prefix = host_prefix
        super(DiskTool, self).__init__(*args, **kwargs)

    def _wapper(self, path):
        if not self.host_prefix:
            return path
        if path[0] == os.path.sep:
            path = path[1:]
        return os.path.join(self.host_prefix, path)

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

    def partitions_create(self, disk, partitions):
        """Create partitions

        :param disk: disk name, eg sda.
        :param partitions: partitions to create, eg ["0%", "20%", "100%"] will
                           create two partitions, 0%-20% and 20%-100%.

        """
        for i in partitions:
            if i[-1] != "%":
                raise ProgrammingError(
                    reason="partitions argument not end with %")
        disk = self._wapper("/dev/%s" % disk)
        cmd = "parted {} mklabel gpt ".format(disk)
        start = partitions.pop(0)
        while True:
            end = partitions.pop(0)
            cmd += " mkpart primary {} {}".format(start, end)
            if len(partitions) == 0:
                break
            start = end
        code, out, err = self.run_command(cmd)
        if code:
            logger.exception("Create partations error, Stdout: %s", out)
            raise ProgrammingError(
                reason="partitions argument not end with %")
        return True

    def partitions_clear(self, disk):
        disk_path = self._wapper("/dev/%s" % disk)
        cmd = "wipefs {} -a".format(disk_path)
        code, out, err = self.run_command(cmd)
        if code:
            logger.exception("Create partations error, Stdout: %s", out)
            raise ProgrammingError(
                reason="partitions argument not end with %")
        return False


if __name__ == '__main__':
    t = DiskTool()
    print(t.all())
