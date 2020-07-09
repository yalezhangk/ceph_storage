#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from oslo_utils import encodeutils


class StorCli:
    def __init__(self, cli_path='storcli64',
                 ssh=None, disk_name=None):
        self.ssh = ssh
        self.cli_path = cli_path
        self.disk_name = disk_name

    def _get_json_output(self, json_databuf):
        outbuf = encodeutils.safe_decode(json_databuf)
        outdata = json.loads(outbuf)
        return outdata

    def get_device_id(self):
        _stdin, _stdout, _stderr = self.ssh.run_command('lsscsi')
        for line in _stdout.split('\n'):
            if self.disk_name in line:
                fields = line.split(':', 5)
                return fields[2]

    def get_eid_slt(self):
        _stdin, _stdout, _stderr = self.ssh.run_command(
            '{} /cALL show all J'.format(self.cli_path))
        device_id = self.get_device_id()

        if not device_id:
            return None
        """
        9:23     28 Onln   5 446.102 GB SATA SSD N   N  512B
                           MZ7LM480HCHP-000V3 00YC396 00YC399LEN U  -
        252:3     7 JBOD  -  446.625 GB SATA SSD N   N  512B
                           INTEL SSDSC2KB480G7 U  -
        """
        out_data = self._get_json_output(_stdout)
        for controller in out_data.get('Controllers'):
            if controller.get("Command Status").get("Status") != "Success":
                return None

            dg_id = None
            for vd in controller.get("Response Data").get("VD LIST"):
                dg_vd = vd.get('DG/VD').split('/')
                if len(dg_vd) == 2 and dg_vd[1] == device_id:
                    dg_id = dg_vd[0]
                    break

            for pd_list in controller.get("Response Data").get("PD LIST"):
                if ((pd_list.get("State") == "Onln" and
                     str(pd_list.get("DG")) == dg_id) or
                    (pd_list.get("State") == "JBOD" and
                     pd_list.get("DID") == int(device_id))):
                    return pd_list.get("EID:Slt").split(':')

    def show_all(self):
        cmd = [self.cli_path, '/c0', 'show', 'all', 'J']
        code, out, err = self.ssh.run_command(cmd)
        if code:
            return None
        out_data = json.loads(encodeutils.safe_decode(out))
        return out_data

    def disk_light(self, cmd):
        es = self.get_eid_slt()
        if not es:
            return False
        _success = False
        _stdin, _stdout, _stderr = self.ssh.run_command(
            '{} /c0/e{}/s{} {} locate'.format(
                self.cli_path, es[0], es[1], cmd))
        for line in _stdout.split('\n'):
            if "Status = Success" in line:
                _success = True
        return _success

    def disk_patrol(self):
        patrol_data = {
            "media_error_count": 0,
            "other_error_count": 0,
            "predictive_failure_count": 0,
            "drive_temperature": 0
        }
        es = self.get_eid_slt()
        if not es:
            return patrol_data
        _stdin, _stdout, _stderr = self.ssh.run_command(
            '{} /c0/e{}/s{} show all J'.format(self.cli_path, es[0], es[1]))
        out_data = self._get_json_output(_stdout)
        detail = "Drive /c0/e{}/s{} - Detailed Information".format(
            es[0], es[1])
        state = "Drive /c0/e{}/s{} State".format(es[0], es[1])
        for controller in out_data.get('Controllers'):
            if controller.get("Command Status").get("Status") != "Success":
                return patrol_data
            detailed_data = controller.get("Response Data").get(detail)
            state_data = detailed_data.get(state)
            patrol_data = {
                "media_error_count": state_data.get("Media Error Count"),
                "other_error_count": state_data.get("Other Error Count"),
                "predictive_failure_count": state_data.get(
                    "Predictive Failure Count"),
                "drive_temperature": state_data.get("Drive Temperature")
            }
        return patrol_data
