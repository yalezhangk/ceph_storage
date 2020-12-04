import logging
import os
import re
import socket
import struct

import six

from DSpace.common.config import CONF
from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.utils import cluster_config

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
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        stdout = stdout.strip()
        nics = list(filter(
            lambda x: x != 'bonding_masters',
            stdout.split('\n')
        ))

        return nics

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

    def get_node_fsstat(self):
        fs_stat = os.statvfs(CONF.host_prefix)
        return fs_stat

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
        os_distro = CONF.os_distro
        pkg_mgr = cluster_config.PKG_MGR[os_distro]
        logger.info("current system distro: %s, pkg_mgr: %s",
                    os_distro, pkg_mgr)
        if pkg_mgr == "yum":
            cmd = ['rpm', '-qa', '|', 'grep', pkg_name]
        elif pkg_mgr == "apt":
            cmd = ['dpkg', '-l', '|', 'grep', pkg_name]
        rc, stdout, stderr = self.run_command(cmd)
        if rc == 0:
            return True
        elif rc == 1:
            return False
        else:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def check_firewall(self):
        cmd = ["systemctl", "status", "firewalld", "|", "grep", "Active",
               "|", "awk", "'{{print $2}}'"]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            result = stdout.strip()
            if result == 'active':
                return False
            else:
                return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def get_all_process(self):
        proc = self._wapper('/proc')
        pids = [pid for pid in os.listdir(proc) if pid.isdigit()]
        processes = []

        for pid in pids:
            try:
                processes.append(open(
                    os.path.join(proc, pid, 'cmdline'), 'rb'
                ).read().split('\0'))
            except IOError:  # proc has already terminated
                continue
        return processes

    def get_process_list(self, match):
        matched = []
        processes = self.get_all_process()
        for i in processes:
            if re.search(match, i[0]):
                matched.append(i)
        return matched

    def ping(self, ip):
        logger.info("Ping ip address %s", ip)
        cmd = "ping {} -c 1".format(ip)
        result = os.system(cmd)
        if result:
            return False
        else:
            return True

    def set_sysctl(self, key, value):
        logger.info("Set sysctl: value %s, key: %s", key, value)
        path = "/etc/sysctl.conf"
        cmd = ["grep", "-r", "^" + key, path]
        rc, stdout, stderr = self.run_command(cmd)
        # if not exit, rc=1, stdout=None, stderr=None
        # if error, rc > 1, stderr != None
        if stderr or rc > 1:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        if stdout:
            change = "'s/{}/{}/g'".format(stdout.strip('\r\n'),
                                          key + "=" + str(value))
            cmd = ["sed", "-i", change, path]
        else:
            change = "'$a\\{}'".format(key + "=" + str(value))
            cmd = ["sed", "-i", change, path]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["sysctl", "-p"]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
