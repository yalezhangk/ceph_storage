#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import os
import socket
import struct

from oslo_log import log as logging

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.tools.utils import get_file_content

logger = logging.getLogger(__name__)


class NetworkTool(ToolBase):

    INTERFACE_TYPE = {
        '1': 'ether',
        '32': 'infiniband',
        '512': 'ppp',
        '772': 'loopback',
        '65534': 'tunnel',
    }

    def __init__(self, *args, **kwargs):
        super(NetworkTool, self).__init__(*args, **kwargs)

    def all(self):
        interfaces = {}
        sys_path = self._wapper('/sys/class/net/*')
        logger.debug("Find network from %s", sys_path)
        for path in glob.glob(sys_path):
            logger.info("Find network: %s", path)
            if not os.path.isdir(path):
                continue
            device = os.path.basename(path)
            logger.info(device)
            interfaces[device] = {'device': device}
            if os.path.exists(os.path.join(path, 'address')):
                macaddress = get_file_content(os.path.join(path, 'address'),
                                              default='')
            if macaddress and macaddress != '00:00:00:00:00:00':
                interfaces[device]['macaddress'] = macaddress
            if os.path.exists(os.path.join(path, 'operstate')):
                interfaces[device]['active'] = get_file_content(
                    os.path.join(path, 'operstate')) != 'down'
            if os.path.exists(os.path.join(path, 'device', 'driver',
                                           'module')):
                interfaces[device]['module'] = os.path.basename(
                    os.path.realpath(os.path.join(
                        path, 'device', 'driver', 'module')))
            if os.path.exists(os.path.join(path, 'type')):
                _type = get_file_content(os.path.join(path, 'type'))
                interfaces[device]['type'] = self.INTERFACE_TYPE.get(_type,
                                                                     'unknown')
            if os.path.exists(os.path.join(path, 'device')):
                interfaces[device]['pciid'] = os.path.basename(
                    os.readlink(os.path.join(path, 'device')))
            if os.path.exists(os.path.join(path, 'speed')):
                speed = get_file_content(os.path.join(path, 'speed'))
                if speed is not None:
                    interfaces[device]['speed'] = int(speed)
            args = ['ip', 'addr', 'show', 'primary', device]
            rc, data, stderr = self.executor.run_command(args)
            if rc:
                raise RunCommandError(cmd=args, return_code=rc,
                                      stdout=data, stderr=stderr)
            interfaces[device].update(self.parse_ip_output(data))
            logger.debug("Find network info %s", interfaces[device])
        return interfaces

    def parse_ip_output(self, data):
        ips = {}
        for line in data.split('\n'):
            if not line:
                continue
            words = line.split()
            logger.info(words)
            broadcast = ''
            if words[0] == 'inet':
                if '/' in words[1]:
                    address, netmask_length = words[1].split('/')
                    if len(words) > 3:
                        broadcast = words[3]
                else:
                    address = words[1]
                    netmask_length = "32"
                address_bin = struct.unpack('!L', socket.inet_aton(address))[0]
                netmask_bin = (1 << 32) - (1 << 32 >> int(netmask_length))
                netmask = socket.inet_ntoa(struct.pack('!L', netmask_bin))
                network = socket.inet_ntoa(
                    struct.pack('!L', address_bin & netmask_bin))
                ips['ipv4'] = {'address': address,
                               'broadcast': broadcast,
                               'netmask': netmask,
                               'network': network}
                break
        return ips


if __name__ == '__main__':
    from DSpace.tools.base import Executor
    from DSpace.common.config import CONF
    logging.setup(CONF, "stor")
    t = NetworkTool(Executor(), host_prefix="")
    print(t.all())
