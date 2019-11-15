import logging
import socket
import struct

import six

from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase

logger = logging.getLogger(__name__)


class System(ToolBase):
    def _parse_ip_output(self, output, net_info, secondary=False):
        for line in output.splitlines():
            if not line:
                continue
            words = line.split()
            broadcast = ''
            if words[0] == 'inet':
                if '/' in words[1]:
                    address, netmask_length = words[1].split('/')
                    if len(words) > 3:
                        broadcast = words[3]
                else:
                    # pointopoint interfaces do not have a prefix
                    address = words[1]
                    netmask_length = "32"
                address_bin = struct.unpack('!L', socket.inet_aton(address))[0]
                netmask_bin = (1 << 32) - (1 << 32 >> int(netmask_length))
                netmask = socket.inet_ntoa(struct.pack('!L', netmask_bin))
                network = socket.inet_ntoa(
                    struct.pack('!L', address_bin & netmask_bin)
                )
                # NOTE: device is ref to outside scope
                # NOTE: interfaces is also ref to outside scope
                if not secondary and "ipv4" not in net_info:
                    net_info['ipv4'] = {'address': address,
                                        'broadcast': broadcast,
                                        'netmask': netmask,
                                        'network': network}
                else:
                    if "ipv4_secondaries" not in net_info:
                        net_info["ipv4_secondaries"] = []
                        net_info["ipv4_secondaries"].append({
                            'address': address,
                            'broadcast': broadcast,
                            'netmask': netmask,
                            'network': network,
                        })
                # add this secondary IP to the main device
                if secondary:
                    if "ipv4_secondaries" not in net_info:
                        net_info["ipv4_secondaries"] = []
                        net_info["ipv4_secondaries"].append({
                            'address': address,
                            'broadcast': broadcast,
                            'netmask': netmask,
                            'network': network,
                        })
            elif words[0] == 'inet6':
                if 'peer' == words[2]:
                    address = words[1]
                    _, prefix = words[3].split('/')
                    scope = words[5]
                else:
                    address, prefix = words[1].split('/')
                    scope = words[3]
                if 'ipv6' not in net_info:
                    net_info['ipv6'] = []
                    net_info['ipv6'].append({
                        'address': address,
                        'prefix': prefix,
                        'scope': scope
                    })

    def _interfaces_retrieve(self):
        cmd = ['ls', '/sys/class/net']
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            stdout = stdout.strip()
            nics = stdout.split('\n')
            return nics
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def _network_retrieve(self, device):
        cmd = ['ip', 'addr', 'show', 'primary', device]
        rc, primary_data, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=primary_data, stderr=stderr)

        cmd = ['ip', 'addr', 'show', 'secondary', device]
        rc, secondary_data, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=secondary_data, stderr=stderr)

        net_info = {}
        primary_data = primary_data.strip()
        secondary_data = secondary_data.strip()
        self._parse_ip_output(primary_data, net_info)
        self._parse_ip_output(secondary_data, net_info, secondary=True)
        return net_info

    def _hostname_retrieve(self):
        cmd = ['hostname']
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            hostname = stdout.strip()
            return hostname
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def get_node_baseinfo(self):
        res = {
            'hostname': self._hostname_retrieve(),
            'networks': []
        }

        nic_info = {}
        devices = self._interfaces_retrieve()
        for device in devices:
            nic_info[device] = self._network_retrieve(device)
        for net_name, net_info in six.iteritems(nic_info):
            if net_name.startswith("vnet"):
                continue
            if net_name.startswith("lo"):
                continue
            if not net_info.get("ipv4"):
                continue
            net = net_info.get('ipv4')
            network = {
                "ip_address": net.get("address"),
                "netmask": net.get("netmask"),
            }
            res['networks'].append(network)

        return res

    def check_selinux(self):
        cmd = ['getenforce']
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            result = stdout.strip()
            if result == 'Disabled':
                return True
            else:
                return False
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def check_package(self, pkg_name):
        cmd = ['rpm', '-qa', '|', 'grep', pkg_name]
        rc, stdout, stderr = self.run_command(cmd)
        if rc == 0:
            return False
        elif rc == 1:
            return True
        else:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
