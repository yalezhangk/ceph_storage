#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import uuid

import six

from DSpace.exception import RunCommandError
from DSpace.objects import fields as s_fields
from DSpace.tools.base import ToolBase
from DSpace.tools.storcli import StorCli as StorCliTool
from DSpace.utils import retry
from DSpace.utils.ptype import PTYPE

logger = logging.getLogger(__name__)


class DiskTool(ToolBase):

    def __init__(self, *args, **kwargs):
        super(DiskTool, self).__init__(*args, **kwargs)

    def _get_disk_mounts(self):
        cmd = ['partprobe']
        code, out, err = self.run_command(cmd)
        if code:
            logger.error("partprobe error: %s %s" % (out, err))
            return
        cmd = ["lsblk", "-P", "-o", "NAME,MOUNTPOINT,TYPE"]
        code, out, err = self.run_command(cmd)
        if code:
            logger.error("lsblk error: %s %s" % (out, err))
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

    def _get_disk_udev_info(self, disk):
        # _disk = self._wapper("/dev/%s" % disk)
        _disk = "/dev/%s" % disk
        cmd = ["udevadm", "info", "-q", "property", _disk]
        code, out, err = self.run_command(cmd)
        if code:
            return
        udev_info = {}
        for attr in out.split('\n'):
            if '=' not in attr:
                continue
            k_v = attr.split('=', 1)
            udev_info.update({k_v[0]: k_v[1]})
        logger.info("get disk %s udev info: %s", disk, udev_info)
        return udev_info

    def update_udev_info(self, disk_name, disk_info):
        udev_info = self._get_disk_udev_info(disk_name)
        dev_bus = udev_info.get("ID_BUS")
        if dev_bus == "scsi":
            slot = udev_info.get("ID_PATH").split("-")[-1]
            disk_info.update({
                "slot": slot,
                "serial": udev_info.get("ID_SCSI_SERIAL"),
                "wwid": udev_info.get("ID_WWN_WITH_EXTENSION"),
            })
        else:
            disk_info.update({"serial": udev_info.get("ID_SERIAL")})
        disk_info.update({
            "model": udev_info.get("ID_MODEL"),
            "vender": udev_info.get("ID_VENDOR"),
        })

    @retry(RunCommandError, interval=0.2, retries=5)
    def get_partition_uuid(self, part):
        disk_part = self._wapper("/dev/%s" % part)
        cmd = ["blkid", disk_part, "-o", "udev"]
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        blk_udev = out.split('\n')
        part_uuid = None
        for udev in blk_udev:
            if "PARTUUID" in udev:
                part_uuid = udev.split('=')[-1][0:36]
                break
        # get system part uuid
        if not part_uuid:
            for udev in blk_udev:
                if "ID_FS_UUID" in udev:
                    part_uuid = udev.split('=')[-1][0:36]
                    break
        return part_uuid

    def get_disk_info(self, disk_name, disk_mounts=None):
        sys_partitions = [self._wapper('/'),
                          self._wapper('/boot'),
                          self._wapper('/home')]
        if not disk_mounts:
            disk_mounts = self._get_disk_mounts()
        path = self._wapper("/sys/class/block/%s" % disk_name)
        dirs = os.listdir(path)
        if 'stat' not in dirs or 'device' not in dirs:
            return None
        guid = self.get_disk_guid(disk_name)
        size = open(os.path.join(path, 'size')).read().strip()
        rotational = open(
            os.path.join(path, 'queue', 'rotational')).read().strip()
        if rotational == '0':
            disk_type = s_fields.DiskType.SSD
        else:
            disk_type = s_fields.DiskType.HDD
        disk_info = {
            "partitions": [],
            "size": int(size) * 512,
            "name": disk_name,
            "guid": guid,
            "type": disk_type
        }
        mount_info = disk_mounts.get(disk_name)
        if mount_info:
            if mount_info['MOUNTPOINT']:
                disk_info['mounted'] = True
                if mount_info['MOUNTPOINT'] in sys_partitions \
                        and mount_info['TYPE'] == 'part':
                    disk_info['is_sys_dev'] = True
        for partition in dirs:
            if not partition.startswith(disk_name):
                continue
            mount_info = disk_mounts.get(partition)
            if mount_info:
                if mount_info['MOUNTPOINT'] in sys_partitions \
                        and mount_info['TYPE'] == 'part':
                    disk_info['is_sys_dev'] = True
            part_uuid = None
            try:
                part_uuid = self.get_partition_uuid(partition)
            except RunCommandError:
                logger.info("Can not get part %s uuid", partition)
            part_size = open(
                os.path.join(path, partition, 'size')
            ).read().strip()
            part_info = {
                "size": int(part_size) * 512,
                "uuid": part_uuid,
                "name": partition
            }
            disk_info['partitions'].append(part_info)
        return disk_info

    def all(self):
        res = {}
        path = self._wapper("/sys/class/block/")
        disk_mounts = self._get_disk_mounts()
        for block in os.listdir(path):
            if block.startswith('sr'):
                continue
            disk_info = self.get_disk_info(block, disk_mounts=disk_mounts)
            if disk_info:
                res[block] = disk_info
        return res

    def partitions_create(self, disk, partitions):
        logger.info('Create disk partition for %s', disk)
        _disk = self._wapper("/dev/%s" % disk)
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
                   partition_guid, type_code, "--mbrtogpt", "--", _disk]
            code, out, err = self.run_command(cmd)
            if code:
                raise RunCommandError(cmd=cmd, return_code=code,
                                      stdout=out, stderr=err)
            self.partprobe(disk)
            part_uuid = self.get_partition_uuid(part['name'])
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
        self.partprobe(disk)
        return True

    def data_clear(self, disk):
        disk_path = self._wapper("/dev/%s" % disk)
        if not os.path.exists(disk_path):
            logger.warning("%s does not exist", disk_path)
            return True
        cmd = "dd if=/dev/zero of={} bs=4M count=30".format(disk_path)
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True

    def partprobe(self, disk=None):
        logger.info('Running cmd: partprobe')
        cmd = ["partprobe"]
        if disk:
            _disk = self._wapper("/dev/%s" % disk)
            cmd.append(_disk)
        code, out, err = self.run_command(cmd)
        if code:
            raise RunCommandError(cmd=cmd, return_code=code,
                                  stdout=out, stderr=err)
        return True

    def get_disk_guid(self, disk):
        logger.debug("Get disk %s GUID", disk)
        _disk = self._wapper("/dev/%s" % disk)
        cmd = ["sgdisk", "-p", _disk]
        code, out, err = self.run_command(cmd)
        if code:
            return None
        guid = None
        for attr in out.split('\n'):
            if "GUID" in attr:
                guid = attr.split(":")[-1].lstrip()
                break
        return guid

    def update_disk_type(self, disks):
        logger.debug("update disk type for raid disk")

        storcli = StorCliTool(ssh=self.executor)
        out_data = storcli.show_all()
        if not out_data:
            return
        vd_list = {}
        pd_list = {}
        for controller in out_data.get('Controllers'):
            if controller.get("Command Status").get("Status") != "Success":
                return
            for vd in controller.get("Response Data").get("VD LIST"):
                dg_vd = vd.get('DG/VD').split('/')
                if len(dg_vd) == 2:
                    vd_list.update({dg_vd[1]: dg_vd[0]})
            for pd in controller.get("Response Data").get("PD LIST"):
                if pd.get('State') == 'Onln':
                    pd_list.update({str(pd.get('DG')): pd.get('Med')})
        if not vd_list:
            return
        for name, data in six.iteritems(disks):
            slot = data.get('slot')
            if slot:
                slot_vd = slot.split(':')[2]
                if slot_vd in vd_list:
                    dg = vd_list.get(slot_vd)
                    data['type'] = pd_list.get(dg).lower()


if __name__ == '__main__':
    from DSpace.tools.base import Executor

    t = DiskTool(Executor())
    print(t.all())
