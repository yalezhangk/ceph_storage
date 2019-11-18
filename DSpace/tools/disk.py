#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

from DSpace.exception import ProgrammingError
from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class DiskTool(ToolBase):
    host_prefix = None

    def __init__(self, *args, **kwargs):
        self.host_prefix = kwargs.pop("host_prefix", None)
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
        cmd = ["parted", disk, "mklabel", "gpt"]
        start = partitions.pop(0)
        while True:
            end = partitions.pop(0)
            cmd += ["mkpart", "primary", start, end]
            if len(partitions) == 0:
                break
            start = end
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True

    def partitions_clear(self, disk):
        disk_path = self._wapper("/dev/%s" % disk)
        cmd = ["wipefs", disk_path, "-a"]
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True

    def data_clear(self, disk):
        disk_path = self._wapper("/dev/%s" % disk)
        cmd = "dd if=/dev/zero of={} bs=4M count=30".format(disk_path)
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True


if __name__ == '__main__':
    from DSpace.tools.base import Executor
    t = DiskTool(Executor())
    print(t.all())
