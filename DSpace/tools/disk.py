#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import uuid

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.utils.ptype import PTYPE

logger = logging.getLogger(__name__)


class DiskTool(ToolBase):

    def __init__(self, *args, **kwargs):
        super(DiskTool, self).__init__(*args, **kwargs)

    def _get_disk_mounts(self):
        cmd = ["lsblk", "-P", "-o", "NAME,MOUNTPOINT,TYPE"]
        code, out, err = self.run_command(cmd)
        if code:
            return
        disk_mounts = {}
        for info in out.split('\n'):
            values = info.split()
            if not values:
                continue
            disk = {}
            for v in values:
                k_v = v.split('=', 1)
                disk.update({k_v[0]: eval(k_v[1])})
            if disk:
                disk_mounts[disk['NAME']] = disk
        logger.info("lsblk detail parsed %s", disk_mounts)
        return disk_mounts

    def all(self):
        res = {}
        path = self._wapper("/sys/class/block/")
        disk_mounts = self._get_disk_mounts()
        sys_partitions = [self._wapper('/'),
                          self._wapper('/boot'),
                          self._wapper('/home')]
        for block in os.listdir(path):
            if block[-1].isdigit():
                continue
            dirs = os.listdir(os.path.join(path, block))
            if 'stat' not in dirs:
                continue
            size = open(os.path.join(path, block, 'size')).read().strip()
            res[block] = {
                "partitions": {},
                "size": int(size) * 512,
                "rotational": open(
                    os.path.join(path, block, 'queue', 'rotational')
                ).read().strip()
            }
            block_mount = disk_mounts.get(block)
            if block_mount:
                if block_mount['MOUNTPOINT']:
                    res[block]['mounted'] = True

            for partition in dirs:
                mount_info = disk_mounts.get(partition)
                if mount_info:
                    if mount_info['MOUNTPOINT'] in sys_partitions \
                            and mount_info['TYPE'] == 'part':
                        res[block]['is_sys_dev'] = True
                if not partition.startswith(block):
                    continue
                res[block]["partitions"][partition] = {
                    "size": open(
                        os.path.join(path, block, partition, 'size')
                    ).read().strip(),
                }
        return res

    def partitions_create(self, disk, partitions):
        logger.info('Create disk partition for %s', disk)
        disk = self._wapper("/dev/%s" % disk)
        order = 1
        role_map = {
            "db": "block.db",
            "wal": "block.wal",
            "cache": "block.t2ce",
            "journal": "journal"
        }
        for part in partitions:
            role = role_map[part["role"]]
            size = part["size"] / 1024
            partition_guid = "--partition-guid={}:{}".format(order,
                                                             str(uuid.uuid4()))
            if len(partitions) == order:
                partition_size = "--largest-new={}".format(order)
            else:
                partition_size = "--new={}:0:+{}K".format(order, int(size))

            type_code = "--typecode={}:{}"\
                .format(order, PTYPE['regular'][role]['ready'])
            cmd = ["sgdisk", partition_size,
                   "--change-name='{}:ceph {}'".format(order, role),
                   partition_guid, type_code, "--mbrtogpt", "--", disk]
            code, out, err = self.run_command(cmd)
            if code:
                raise RunCommandError(cmd=cmd, return_code=code,
                                      stdout=out, stderr=err)
            order += 1
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
