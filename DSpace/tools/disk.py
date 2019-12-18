#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import re
import uuid

from DSpace.exception import RunCommandError
from DSpace.objects import fields as s_fields
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

            cmd = ["lsscsi"]
            code, out, err = self.run_command(cmd)
            if code:
                raise RunCommandError(cmd=cmd, return_code=code,
                                      stdout=out, stderr=err)
            out = out.split('\n')
            for scsi in out:
                if block in scsi:
                    slot = re.search(r"(?<=\[).*?(?=\])", scsi).group()
                    break

            size = open(os.path.join(path, block, 'size')).read().strip()
            res[slot] = {
                "partitions": {},
                "size": int(size) * 512,
                "slot": slot,
                "name": block,
                "rotational": open(
                    os.path.join(path, block, 'queue', 'rotational')
                ).read().strip()
            }
            block_mount = disk_mounts.get(block)
            if block_mount:
                if block_mount['MOUNTPOINT']:
                    res[slot]['mounted'] = True

            for partition in dirs:
                mount_info = disk_mounts.get(partition)
                if mount_info:
                    if mount_info['MOUNTPOINT'] in sys_partitions \
                            and mount_info['TYPE'] == 'part':
                        res[slot]['is_sys_dev'] = True
                if not partition.startswith(block):
                    continue
                part_size = open(
                    os.path.join(path, block, partition, 'size')
                ).read().strip()
                res[slot]["partitions"][partition] = {
                    "size": int(part_size) * 512,
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
            self.partprobe()
            disk_part = self._wapper("/dev/%s" % part['name'])
            cmd = ["blkid", disk_part, "-o", "udev"]
            code, out, err = self.run_command(cmd)
            if code:
                raise RunCommandError(cmd=cmd, return_code=code,
                                      stdout=out, stderr=err)
            blk_udev = out.split('\n')
            part_uuid = None
            for udev in blk_udev:
                if "PARTUUID" in udev:
                    part_uuid = udev.split('=')[-1]
            part['uuid'] = part_uuid
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

    def partprobe(self):
        logger.info('Running cmd: partprobe')
        cmd = ["partprobe"]
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True

    def get_disk_info_by_slot(self, slot):
        path = self._wapper("/sys/class/scsi_disk/%s/device/block/" % slot)
        block = os.listdir(path)[0]
        dirs = os.listdir(os.path.join(path, block))
        size = open(os.path.join(path, block, 'size')).read().strip()
        rotational = open(
            os.path.join(path, block, 'queue', 'rotational')).read().strip()
        if rotational == '0':
            disk_type = s_fields.DiskType.SSD
        else:
            disk_type = s_fields.DiskType.HDD
        disk_info = {
            "size": int(size) * 512,
            "slot": slot,
            "name": block,
            "type": disk_type
        }
        partitions = []
        for partition in dirs:
            if not partition.startswith(block):
                continue
            part_size = open(
                os.path.join(path, block, partition, 'size')
            ).read().strip()
            disk_part = self._wapper("/dev/%s" % partition)
            cmd = ["blkid", disk_part, "-o", "udev"]
            code, out, err = self.run_command(cmd)
            if code:
                raise RunCommandError(cmd=cmd, return_code=code,
                                      stdout=out, stderr=err)
            blk_udev = out.split('\n')
            uuid = None
            for udev in blk_udev:
                if "PARTUUID" in udev:
                    uuid = udev.split('=')[-1]
            partition = {
                'size': int(part_size) * 512,
                'name': partition,
                'uuid': uuid
            }
            partitions.append(partition)
        disk_info['partitions'] = partitions
        return disk_info


if __name__ == '__main__':
    from DSpace.tools.base import Executor

    t = DiskTool(Executor())
    print(t.all())
