#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import ConfigParser
import errno
import json
import os
import re
import subprocess

import yum

ALLOWED_FAULT_DOMAIN = ['root', 'rack', 'datacenter', 'host', 'osd']
PKGS = ['ceph-common', 'ceph-base', 'ceph-mgr', 'ceph-osd', 'python-cephfs',
        'ceph-selinux', 'ceph-mds', 'libcephfs2', 'ceph-mon', 'librbd1',
        'librados2']


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


def _get_networks():
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
    res = {
        'config': _get_networks(),
        'nodes': nodes
    }
    return res


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


def _get_pkg_version_by_yum(pkg):
    yb = yum.YumBase()
    yb.doConfigSetup(init_plugins=False)
    installed = None
    available = None
    data = yb.doPackageLists(pkgnarrow='all', patterns=PKGS,
                             showdups=True)
    for item in data.available:
        available = {
            "version": item.version,
            "release": item.release.split('.')[0]
        }
        break
    for item in data.installed:
        installed = {
            "version": item.version,
            "release": item.release.split('.')[0]
        }
        break

    return {
        "installed": installed,
        "available": available,
    }


def get_pkg_version(pkg):
    return _get_pkg_version_by_yum(pkg)


def get_ceph_version():
    for pkg in PKGS:
        v = get_pkg_version(pkg)
        if v:
            return v
    return None


def include_check(args):
    response = {
    }
    if args.ceph_version:
        response["ceph_version"] = get_ceph_version()
    return response


def main():
    # TODO: Merge check
    parser = argparse.ArgumentParser(description='Node info collect.')
    parser.add_argument("action")
    parser.add_argument('--ceph_version', action='store_true',
                        help='get ceph version')
    args = parser.parse_args()
    action = args.action
    if action == "collect_nodes":
        data = collect_nodes()
        print(json.dumps(data))
    elif action == "ceph_services":
        data = collect_ceph_services()
        print(json.dumps(data))
    elif action == "ceph_osd":
        # TODO: get osd info
        data = collect_osd_info()
        print(json.dumps(data))
    elif action == "ceph_config":
        data = collect_ceph_config()
        print(json.dumps(data))
    elif action == "ceph_keyring":
        data = collect_ceph_keyring()
        print(json.dumps(data))
    elif action == "check_planning":
        data = check_planning()
        print(json.dumps(data))
    elif action == "check":
        # TODO: move all check to include_check
        data = include_check(args)
        print(json.dumps(data))


if __name__ == '__main__':
    main()
