#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import subprocess


def _bytes2str(string):
    return string.decode('utf-8') if isinstance(string, bytes) else string


def run_command(args, timeout=None):
    cmd = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = cmd.communicate()
    rc = cmd.returncode
    return (rc, _bytes2str(stdout), _bytes2str(stderr))


def _get_networks():
    """Collect info from ceph.conf

    1. public_network
    2. cluster_network
    """
    cmd = ["ceph-conf", "--lookup", "public_network"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    public_network = out.strip()

    cmd = ["ceph-conf", "--lookup", "cluster_network"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    cluster_network = out.strip()
    return {
        "public_network": public_network,
        "cluster_network": cluster_network,
    }


def _collect_mon_nodes():
    cmd = ["ceph", "mon", "dump", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    res = json.loads(out)
    mons = res.get('mons')
    if not mons:
        return None

    mon_hosts = []
    for mon in mons:
        public_addr = mon.get('public_addr')
        public_addr = public_addr.split(':')[0]
        mon_hosts.append(public_addr)

    return mon_hosts


def _collect_mgr_nodes():
    cmd = ["ceph", "mgr", "dump", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    res = json.loads(out)
    mgr_addr = res.get('active_addr')
    if not mgr_addr or mgr_addr == '-':
        return None

    mgr_addr = mgr_addr.split(':')[0]
    mgr_hosts = []
    mgr_hosts.append(mgr_addr)

    return mgr_hosts


def _collect_osd_nodes():
    cmd = ["ceph", "osd", "dump", "--format", "json"]
    rc, out, err = run_command(cmd)
    if rc:
        raise Exception("Cmd run failed: %s", cmd)
    res = json.loads(out)
    osds = res.get('osds')
    if not osds:
        return None

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
    return {
        'config': _get_networks(),
        'mons': _collect_mon_nodes(),
        'mgrs': _collect_mgr_nodes(),
        'osds': _collect_osd_nodes()
    }


def main():
    data = collect_nodes()
    print(json.dumps(data))


if __name__ == '__main__':
    main()
