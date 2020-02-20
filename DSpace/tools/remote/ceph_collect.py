#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import ConfigParser
import errno
import json
import os
import re
import socket
import struct
import subprocess
import sys

try:
    import yum
    pkg_mgr = "yum"
except ImportError:
    yum = None

try:
    import apt
    pkg_mgr = "apt"
except ImportError:
    apt = None

try:
    import dbus
except ImportError:
    dbus = None

ALLOWED_FAULT_DOMAIN = ['root', 'rack', 'datacenter', 'host', 'osd']
PKGS = ['ceph-common', 'ceph-base', 'ceph-mgr', 'ceph-osd', 'python-cephfs',
        'ceph-selinux', 'ceph-mds', 'libcephfs2', 'ceph-mon', 'librbd1',
        'librados2']
CEPH_SYSTEMD_DIRS = [
    "/etc/systemd/system/ceph-mds.target.wants",
    "/etc/systemd/system/ceph-mgr.target.wants",
    "/etc/systemd/system/ceph-radosgw.target.wants",
    "/etc/systemd/system/ceph.target.wants"
]


def _bytes2str(string):
    return string.decode('utf-8') if isinstance(string, bytes) else string


def run_command(args, timeout=None):
    try:
        cmd = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = cmd.communicate()
        rc = cmd.returncode
        return (rc, _bytes2str(stdout), _bytes2str(stderr))
    except Exception:
        return (1, "", "Command failed %s" % args)


def read_one_line(parent, name):
    path = os.path.join(parent, name)
    try:
        line = open(path, 'rb').read()
    except IOError as e:
        if e.errno == errno.ENOENT:
            return None
        else:
            raise

    line = _bytes2str(line)

    if line[-1:] != '\n':
        return line
    line = line[:-1]
    return line


def get_all_process():
    proc = '/proc'
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


def get_process_list(match):
    matched = []
    processes = get_all_process()
    for i in processes:
        if(re.search(match, i[0])):
            matched.append(i)
    return matched


def get_networks():
    """Collect info from ceph.conf

    1. public_network
    2. cluster_network
    """
    cmd = ["ceph-conf", "--lookup", "public_network"]
    rc, out, err = run_command(cmd)
    if rc:
        public_network = None
    else:
        public_network = out.strip()

    cmd = ["ceph-conf", "--lookup", "cluster_network"]
    rc, out, err = run_command(cmd)
    if rc:
        cluster_network = None
    else:
        cluster_network = out.strip()
    return {
        "public_network": public_network,
        "cluster_network": cluster_network,
    }


def _collect_mon_nodes():
    cmd = ["ceph", "mon", "dump", "--connect-timeout", "1", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        return []
    res = json.loads(out)
    mons = res.get('mons')
    if not mons:
        return []

    mon_hosts = []
    for mon in mons:
        public_addr = mon.get('public_addr')
        public_addr = public_addr.split(':')[0]
        mon_hosts.append(public_addr)

    return mon_hosts


def _collect_mgr_nodes():
    cmd = ["ceph", "mgr", "dump", "--connect-timeout", "1", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        return []
    res = json.loads(out)
    mgr_addr = res.get('active_addr')
    if not mgr_addr or mgr_addr == '-':
        return []

    mgr_addr = mgr_addr.split(':')[0]
    mgr_hosts = []
    mgr_hosts.append(mgr_addr)

    return mgr_hosts


def _collect_osd_nodes():
    cmd = ["ceph", "osd", "dump", "--connect-timeout", "1", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        return []
    res = json.loads(out)
    osds = res.get('osds')
    if not osds:
        return []

    osd_hosts = []
    for osd in osds:
        public_addr = osd.get('public_addr')
        if public_addr == '-':
            continue
        public_addr = public_addr.split(':')[0]
        osd_hosts.append(public_addr)
    osd_hosts = list(set(osd_hosts))

    return osd_hosts


def collect_nodes():
    nodes = {}
    for node in _collect_osd_nodes():
        if node not in nodes:
            nodes[node] = {}
        nodes[node]['osd'] = True
    for node in _collect_mgr_nodes():
        if node not in nodes:
            nodes[node] = {}
        nodes[node]['mgr'] = True
    for node in _collect_mon_nodes():
        if node not in nodes:
            nodes[node] = {}
        nodes[node]['mon'] = True
    return nodes


def collect_ceph_services():
    services = []
    for i in ["ceph-mon", "ceph-osd", "ceph-mgr", "ceph-radosgw"]:
        res = get_process_list(i)
        if res:
            services.append(i)
    return services


def realpath(parent, name):
    return os.path.realpath(os.path.join(parent, name))


def realpart(parent, name):
    dev = realpath(parent, name)
    return dev.rsplit('/', 1)[1]


def file_exists(parent, name):
    return os.path.exists(os.path.join(parent, name))


def _collect_osd_detail(osd):
    path = osd.get('path')
    osd['type'] = read_one_line(path, 'type')
    osd['fsid'] = read_one_line(path, 'fsid')
    osd['ceph_fsid'] = read_one_line(path, 'ceph_fsid')
    osd['osd_id'] = read_one_line(path, 'whoami')
    if osd['type'] == 'bluestore':
        if file_exists(path, "block.db"):
            osd['block.db'] = realpart(path, "block.db")
        if file_exists(path, "block.wal"):
            osd['block.wal'] = realpart(path, "block.wal")
        if file_exists(path, "block.t2ce"):
            osd['block.t2ce'] = realpart(path, "block.t2ce")
    if osd['type'] == 'filestore':
        if file_exists(path, "journal"):
            journal = realpart(path, "journal")
            if journal != 'journal':
                osd['journal'] = realpart(path, "journal")


def collect_osd_info():
    cmd = ["lsblk", "-P", "-o", "NAME,MOUNTPOINT,TYPE"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    res = []
    for line in out.split('\n'):
        if "osd" not in line:
            continue
        meta = {}
        values = line.split()
        if not values:
            continue
        for v in values:
            k_v = v.split('=', 1)
            meta[k_v[0]] = k_v[1]
        osd_id = meta['MOUNTPOINT'].split('-')[1][:-1]
        disk = meta["NAME"][1:-2]
        osd = {
            "osd_id": osd_id,
            "disk": disk,
            "path": meta['MOUNTPOINT'][1:-1]
        }
        _collect_osd_detail(osd)
        res.append(osd)
    return res


def collect_ceph_config():
    configs = {}
    config = ConfigParser.ConfigParser()
    config.read('/etc/ceph/ceph.conf')
    for section in config.sections():
        configs[section] = {}
        for key in config.options(section):
            configs[section][key] = config.get(section, key)
    return configs


def collect_ceph_keyring():
    cmd = ["ceph", "auth", "get", "client.admin", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        return None
    res = json.loads(out)
    return {
        "entity": res[0]['entity'],
        "key": res[0]['key']
    }


def check_planning():
    response = {
        "check": True,
        "change": []
    }
    cmd = ['ceph', 'osd', 'crush', 'tree', "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        return {
            "check": False
        }
    res = json.loads(out)
    nodes = res['nodes']
    for node in nodes:
        if 'type' in node:
            if node['type'] not in ALLOWED_FAULT_DOMAIN:
                response["change"].append(node['name'])
    return response


def _setup_dns_resolve(timeout, retries):
    content = ("$a options timeout:{} attempts:{} rotate "
               "single-request-reopen").format(timeout, retries)
    cmd = ['sed', '-i', content, '/etc/resolv.conf']
    run_command(cmd)


def _reset_dns_resolve():
    content = "/options timeout.*/d"
    cmd = ['sed', '-i', content, '/etc/resolv.conf']
    run_command(cmd)


def _get_pkg_version_by_apt(pkg):
    resolve_timeout = 1.5
    retries = 1
    _setup_dns_resolve(resolve_timeout, retries)
    pkg_cache = apt.Cache()
    installed = None
    available = None
    unavailable = False
    try:
        pkg = pkg_cache[pkg]
        if pkg.is_installed:
            # installed
            pkg_version = pkg.installed.version
            installed = {
                "version": pkg_version.split("-")[0],
                "release": pkg_version.split("-")[1]
            }
        else:
            # available
            pkg_version = pkg.candidate.version
            available = {
                "version": pkg_version.split("-")[0],
                "release": pkg_version.split("-")[1]
            }
    except KeyError:
        unavailable = True

    _reset_dns_resolve()

    return {
        "installed": installed,
        "available": available,
        "unavailable": unavailable
    }


def _get_pkg_version_by_yum(pkg):
    # yum clean all
    cmd = ['yum', 'clean', 'all']
    run_command(cmd)

    resolve_timeout = 1.5
    retries = 1
    _setup_dns_resolve(resolve_timeout, retries)

    # get package info
    yb = yum.YumBase()
    yb.doConfigSetup(init_plugins=False)
    yb.conf.timeout = 1.5
    yb.conf.retries = 1
    installed = None
    available = None
    unavailable = False

    try:
        data = yb.pkgSack.returnPackages(patterns=PKGS)
        for item in data:
            available = {
                "version": item.version,
                "release": item.release.split('.')[0]
            }
            break
    except Exception:
        unavailable = True

    data = yb.doPackageLists(pkgnarrow='installed', patterns=PKGS)
    for item in data.installed:
        installed = {
            "version": item.version,
            "release": item.release.split('.')[0]
        }
        break
    _reset_dns_resolve()

    return {
        "installed": installed,
        "available": available,
        "unavailable": unavailable
    }


def get_pkg_version(pkg):
    if pkg_mgr == "yum":
        return _get_pkg_version_by_yum(pkg)
    elif pkg_mgr == "apt":
        return _get_pkg_version_by_apt(pkg)


def get_ceph_version():
    for pkg in PKGS:
        v = get_pkg_version(pkg)
        if v:
            return v
    return None


def ceph_is_installed():
    cmd = ["ceph", "-v"]
    rc, stdout, stderr = run_command(cmd)
    if rc == 0:
        return True
    v = get_ceph_version()
    if v["installed"]:
        return True
    if os.path.isdir("/var/lib/ceph/"):
        return True
    if os.path.isdir("/etc/ceph/"):
        return True
    for i in CEPH_SYSTEMD_DIRS:
        if os.path.isdir(i):
            return True
    return False


def selinux_is_enable():
    cmd = ['getenforce']
    rc, stdout, stderr = run_command(cmd)
    result = stdout.strip()
    if rc == 1:
        return False
    if result == 'Disabled':
        return False
    else:
        return True


def get_listen_ports():
    net_tcp = ["tcp", "tcp6"]
    ports = set()
    for i in net_tcp:
        content = open("/proc/net/{}".format(i), "r")
        for line in content:
            if "sl" in line:
                continue
            line = line.strip()
            cols = line.split(" ")
            # check port
            status = cols[3]
            if status != "0A":
                # 0A is listening
                continue
            local_address = cols[1]
            ip, port = local_address.split(":")
            port = int(port, 16)
            ports.add(port)
    return list(ports)


def interfaces_retrieve():
    cmd = ['ls', '/sys/class/net']
    rc, stdout, stderr = run_command(cmd)
    if not rc:
        stdout = stdout.strip()
        nics = stdout.split('\n')
        return nics
    return None


def _parse_ip(output):
    ips = []
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
            ips.append({'address': address,
                        'broadcast': broadcast,
                        'netmask': netmask,
                        'network': network})
    return ips


def network_retrieve(device):
    cmd = ['ip', 'addr', 'show', device]
    rc, out, stderr = run_command(cmd)
    return _parse_ip(out)


def all_ips():
    nic_info = {}
    devices = interfaces_retrieve()
    for device in devices:
        nic_info[device] = network_retrieve(device)
    return nic_info


def service_status_by_dbus(service):
    bus = dbus.SystemBus()
    systemd = bus.get_object('org.freedesktop.systemd1',
                             '/org/freedesktop/systemd1')
    manager = dbus.Interface(
        systemd, dbus_interface='org.freedesktop.systemd1.Manager')
    try:
        unit = manager.GetUnit(service)
    except dbus.exceptions.DBusException as e:
        if "not loaded" in str(e):
            return "inactive"
        raise e

    unit_proxy = bus.get_object('org.freedesktop.systemd1', str(unit))
    unit_properties = dbus.Interface(
        unit_proxy, dbus_interface='org.freedesktop.DBus.Properties')
    res = unit_properties.Get('org.freedesktop.systemd1.Unit', 'ActiveState')
    return str(res)


def service_status_by_cli(service):
    cmd = ["systemctl", "status", service]
    rc, out, stderr = run_command(cmd)
    if "Active: active" in out:
        return 'active'
    return "inactive"


def service_status(service):
    if dbus:
        return service_status_by_dbus(service)
    else:
        return service_status_by_cli(service)


def all_container():
    cmd = ['docker', 'ps', '-a']
    rc, out, stderr = run_command(cmd)
    containers = []
    for line in out.splitlines():
        if "CONTAINER" in line:
            continue
        items = line.split(" ")
        containers.append(items[-1])
    return containers


def check(args):
    response = {}
    if args.ceph_version:
        response["ceph_version"] = get_ceph_version()
    if args.hostname:
        response["hostname"] = socket.gethostname()
    if args.ceph_package:
        response["ceph_package"] = ceph_is_installed()
    if args.selinux:
        response["selinux"] = selinux_is_enable()
    if args.ports:
        response["ports"] = get_listen_ports()
    if args.network:
        response["network"] = all_ips()
    if args.firewall:
        response["firewall"] = service_status("firewalld.service")
    if args.containers:
        response["containers"] = all_container()
    if args.ceph_service:
        response["ceph_service"] = collect_ceph_services()
    return response


def cluster_check(args):
    response = {}
    response["check_crush"] = check_planning()
    response["nodes"] = collect_nodes()
    response["configs"] = get_networks()
    response["services"] = collect_ceph_services()
    return response


def stdout_print(data):
    print(json.dumps(data), file=sys.__stdout__)


def main():
    # TODO: Merge check
    sys.stdout = sys.stderr
    parser = argparse.ArgumentParser(description='Node info collect.')
    parser.add_argument("action")
    parser.add_argument('--ceph_version', action='store_true',
                        help='get ceph version')
    parser.add_argument('--hostname', action='store_true',
                        help='get hostname')
    parser.add_argument('--ceph_package', action='store_true',
                        help='get ceph package')
    parser.add_argument('--selinux', action='store_true',
                        help='get selinux')
    parser.add_argument('--ports', action='store_true',
                        help='get ports')
    parser.add_argument('--network', action='store_true',
                        help='get network')
    parser.add_argument('--firewall', action='store_true',
                        help='get firewall')
    parser.add_argument('--containers', action='store_true',
                        help='get containers')
    parser.add_argument('--ceph_service', action='store_true',
                        help='get ceph service')
    args = parser.parse_args()
    action = args.action
    if action == "collect_nodes":
        data = collect_nodes()
        stdout_print(data)
    elif action == "ceph_services":
        data = collect_ceph_services()
        stdout_print(data)
    elif action == "ceph_osd":
        # TODO: get osd info
        data = collect_osd_info()
        stdout_print(data)
    elif action == "ceph_config":
        data = collect_ceph_config()
        stdout_print(data)
    elif action == "ceph_keyring":
        data = collect_ceph_keyring()
        stdout_print(data)
    elif action == "check_planning":
        data = check_planning()
        stdout_print(data)
    elif action == "check":
        # TODO: move all check to check function
        data = check(args)
        stdout_print(data)
    elif action == "cluster_check":
        # TODO: move all cluster check to check function
        data = cluster_check(args)
        stdout_print(data)


if __name__ == '__main__':
    main()
