#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

from t2stor.tools.base import ToolBase
from t2stor.exception import RunCommandError

logger = logging.getLogger(__name__)


class Ceph(ToolBase):
    def get_mgrs(self):
        logger.debug("detect active mgr from cluster")

        cmd = "ceph mgr dump --format json-pretty"
        rc, stdout, stderr = self.run_command(cmd, timeout=1)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        mgr_map = json.loads(stdout)
        mgr_addr = mgr_map.get('active_addr')
        if not mgr_addr or mgr_addr == '-':
            return None

        mgr_addr = mgr_addr.split(':')[0]
        mgr_hosts = []
        mgr_hosts.append(mgr_addr)

        return mgr_hosts

    def get_mons(self):
        logger.debug("detect mons from cluster")

        cmd = "ceph mon dump --format json-pretty"
        rc, stdout, stderr = self.run_command(cmd, timeout=1)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        mon_map = json.loads(stdout)
        mons = mon_map.get('mons')
        if not mons:
            return None

        mon_hosts = []
        for mon in mons:
            public_addr = mon.get('public_addr')
            public_addr = public_addr.split(':')[0]
            mon_hosts.append(public_addr)

        return mon_hosts

    def get_osds(self):
        logger.debug("detect osds from cluster")

        cmd = "ceph osd dump --format json-pretty"
        rc, stdout, stderr = self.run_command(cmd, timeout=1)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        osd_map = json.loads(stdout)
        osds = osd_map.get('osds')
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
