#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import json
import logging
import os
import time

import six
from oslo_utils import encodeutils

from DSpace.common.config import CONF
from DSpace.exception import ActionTimeoutError
from DSpace.exception import CephCommandTimeout
from DSpace.exception import CephConnectTimeout
from DSpace.exception import CephException
from DSpace.exception import CrushMapNotFound
from DSpace.exception import DeviceOrResourceBusy
from DSpace.exception import PermissionDenied
from DSpace.exception import RunCommandError
from DSpace.exception import SystemctlRestartError
from DSpace.i18n import _
from DSpace.objects.fields import PoolType
from DSpace.tools.base import Executor
from DSpace.tools.base import ToolBase
from DSpace.utils import retry

try:
    import rados
    import rbd
except ImportError:
    rados = None
    rbd = None


logger = logging.getLogger(__name__)
DEFAULT_POOL = 'rbd'
REQUIRED_VERSION = 'luminous'
EC_POOL_RELATION_RE_POOL = '-link_by_ec_pool'
ECP_NAME_END = '-ecp'
"""
eg:  ec_pool: ec_pool_asdf
     ec_crush_rule: rule_01
     re_pool: ec_pool_asdf-link_by_ec_pool
     re_crush_rule: rule_01-link_by_ec_pool
     ecp_name = rule_01-ecp
"""
RULE_TYPE_MAP = {
    1: "replicated",
    3: "erasure",
}
SUPPORTED_TUNABLES = [
    "choose_local_tries",
    "choose_local_fallback_tries",
    "choose_total_tries",
    "chooseleaf_descend_once",
    "chooseleaf_vary_r",
    "chooseleaf_stable",
    "straw_calc_version",
    "allowed_bucket_algs"
]
DEFAULT_CURSH_PATH = "/tmp/crush"


class CephTool(ToolBase):
    """Deprecated Ceph Tool

    All function need move to rados client
    """

    def get_networks(self):
        logger.debug("detect cluster networks")

        cmd = "ceph-conf --lookup cluster_network"
        rc, stdout, stderr = self.run_command(
            cmd, timeout=1, root_permission=False)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cluster_network = stdout

        cmd = "ceph-conf --lookup public_network"
        rc, stdout, stderr = self.run_command(
            cmd, timeout=1, root_permission=False)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        public_network = stdout

        return cluster_network, public_network

    def check_ceph_is_installed(self):
        cmd = ["ceph", "-v"]
        rc, stdout, stderr = self.run_command(cmd)
        if rc == 0:
            return True
        elif rc == 127:
            return False
        else:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def check_mon_is_joined(self, public_ip):
        cmd = ["ceph", "mon", "stat"]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc == 0:
            stdout = stdout.strip()
            if public_ip in stdout:
                return True
        return False

    def module_enable(self, module, hostname, public_ip, mgr_dspace_port):
        # set addr
        addr_key = 'mgr/dspace/{}/server_addr'.format(hostname)
        cmd_addr = ['ceph', 'config-key', 'set', addr_key, public_ip]
        rc, stdout, stderr = self.run_command(cmd_addr, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd_addr, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        logger.info('set ceph config-key success, key:{},value:{}'.format(
            addr_key, public_ip))
        # set port
        cmd_port = ['ceph', 'config-key', 'set', 'mgr/dspace/server_port',
                    mgr_dspace_port]
        rc, stdout, stderr = self.run_command(cmd_port, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd_port, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        logger.info('set ceph config-key success, key:{}, value:{}'.format(
            'mgr/dspace/server_port', mgr_dspace_port))
        # module enable dspace
        cmd = ["ceph", "mgr", "module", "enable", module]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    # all monitor use mon. keyring
    def _init_mon_key(self, mon_secret, keyring_path):
        mon_cap = "mon 'allow *'"
        cmd = ["ceph-authtool", "--create-keyring", keyring_path,
               "--name mon.", "--cap", mon_cap, "--mode 0644",
               "--add-key", mon_secret]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def create_mgr_keyring(self, mgr_id):
        entity = "mgr.{}".format(mgr_id)
        keyring_path = "/var/lib/ceph/mgr/ceph-{}/keyring".format(mgr_id)
        cmd = ["ceph", "auth", "get-or-create", entity,
               "mon", "'allow *'", "osd", "'allow *'", "mds", "'allow *'",
               "-o", keyring_path]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def create_mds_keyring(self, mds_id):
        entity = "mds.{}".format(mds_id)
        keyring_path = "/var/lib/ceph/mds/ceph-{}/keyring".format(mds_id)
        cmd = ["ceph", "auth", "get-or-create", entity,
               "mon", "'allow rwx'", "osd", "'allow *'", "mds", "'allow *'",
               "-o", keyring_path]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def create_rgw_keyring(self, rgw_id, rgw_data_dir):
        keyring_path = "{}/keyring".format(rgw_data_dir)
        entity = "client.rgw.{}".format(rgw_id)
        cmd = ["ceph", "auth", "get-or-create", entity,
               "mon", "'allow *'", "osd", "'allow *'", "mgr", "'allow *'",
               "-o", keyring_path]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    # Get or generate client.admin keyring and bootstrap keyrings
    def create_keys(self, hostname, cluster="ceph"):
        cmd = ["ceph-create-keys", "--cluster", cluster, "-i", hostname]
        logger.info("create admin and bootstrap keyrings")
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            logger.error("create admin and bootstrap keyrings error")
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def collect_keyring(self, entity):
        cmd = ["ceph", "auth", "get", entity, "--format", "json"]
        rc, stdout, stderr = self.run_command(cmd)
        if rc:
            logger.error("can't get keyring for %s, stderr: %s",
                         entity, stderr)
            return None
        res = json.loads(stdout)
        return res[0]['key']

    def mon_install(self, hostname, fsid, mon_secret=None):
        cmd = ["ceph-mon", "--cluster", "ceph", "--setuser",
               "ceph", "--setgroup", "ceph", "--mkfs", "-i",
               hostname, "--fsid", fsid]
        if mon_secret:
            mon_keyring = "/tmp/ceph.mon.keyring"
            self._init_mon_key(mon_secret, mon_keyring)
            cmd.extend(["--keyring", mon_keyring])
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def mon_remove(self, monitor_name):
        cmd = ['ceph', 'mon', 'remove', monitor_name]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if not rc:
            return True
        if "refusing removal of last monitor" in stderr:
            return False
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def disk_clear_partition_table(self, diskname):
        cmd = ['wipefs', '-a', "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ['sgdisk', '-o', "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def disk_prepare(self, backend, diskname, db_partition=None,
                     wal_partition=None, cache_partition=None,
                     journal_partition=None, fsid=None, osd_id=None):
        # dspace-disk --setuser root --setgroup root prepare
        # --osd-uuid {{ osd_fsid }} --osd-id {{ osd_id}}
        # --cluster {{ cluster }} --{{ store_backend }}
        # --block.db {{ item.0 }} --block.t2ce {{ item.1 }} {{ item.2 }}"
        cmd = ["dspace-disk", "--setuser", "ceph", "--setgroup", "ceph",
               "prepare", "--no-locking"]
        if fsid and osd_id:
            cmd.extend(['--osd-uuid', fsid, "--osd-id", osd_id])
        cmd.extend(["--cluster", "ceph", "--%s" % backend])
        if cache_partition:
            cmd.extend(['--block.t2ce', "/dev/%s" % cache_partition])
        if db_partition:
            cmd.extend(['--block.db', "/dev/%s" % db_partition])
        if wal_partition:
            cmd.extend(['--block.wal', "/dev/%s" % wal_partition])
        cmd.append("/dev/%s" % diskname)
        if journal_partition:
            cmd.extend(["/dev/%s" % journal_partition])
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def disk_get_partition_name(self, diskname, part_num):
        part_name = None
        path = self._wapper("/sys/class/block/")
        sys_entry = os.path.join(path, diskname)
        for f in os.listdir(sys_entry):
            if f.startswith(diskname) and f.endswith(str(part_num)):
                # we want the shortest name that starts with the base name
                # and ends with the partition number
                if not part_name or len(f) < len(part_name):
                    part_name = f
        return part_name

    @retry(RunCommandError)
    def disk_active(self, diskname):
        part_name = self.disk_get_partition_name(diskname, '1')
        if not part_name:
            part_name = "/dev/%s1" % diskname
        else:
            part_name = "/dev/%s" % part_name
        cmd = ["dspace-disk", "activate", part_name]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    @retry(RunCommandError)
    def update_typecode(self, diskname):
        cmd = ["dspace-disk", "update-typecode", "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_deactivate(self, diskname):
        part_name = self.disk_get_partition_name(diskname, '1')
        if not part_name:
            part_name = "/dev/%s1" % diskname
        else:
            part_name = "/dev/%s" % part_name

        cmd = ["dspace-disk", "deactivate", part_name]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    @retry(RunCommandError)
    def osd_zap(self, diskname):
        if not diskname:
            logger.warning("Device is None")
            return True
        cmd = ["dspace-disk", "zap", "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if "No such file or directory" in stderr:
            logger.warning("Device %s not found", diskname)
            return True
        if "Device or resource busy" in stderr:
            raise DeviceOrResourceBusy()
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return True

    def osd_mark_out(self, osd_id):
        cmd = ["ceph", "osd", "out", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        # TODO: config remove by user
        if rc == 1 and "error calling conf_read_file" in stderr:
            return
        elif rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    @retry(PermissionDenied)
    def osd_deactivate_flag(self, osd_id):
        mount = "/var/lib/ceph/osd/ceph-%s/" % osd_id
        for f in ['active', 'ready']:
            cmd = ["rm", "-rf", mount + f]
            rc, stdout, stderr = self.run_command(cmd, timeout=5)
        flag = mount + "deactive"
        cmd = ["su", "-", "ceph", "-s", "/bin/bash", "-c", "'touch %s'" % flag]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc == 1 and "No such file" in stderr:
            return
        if "Permission denied" in stderr:
            raise PermissionDenied()
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def systemctl_restart(self, types, service, retrys=10):
        logger.info("Try to restart service type %s, service %s",
                    types, service)
        if types == "rgw":
            check_cmd = ["ps", "-ef", "|", "grep", types, "|", "grep",
                         "client.rgw.%s" % service, "|grep", "-v", "grep", "|",
                         "awk", "'{print $2}'"]
            status_cmd = ["systemctl", "status",
                          "ceph-radosgw@rgw.%s" % (service), "|", "grep",
                          "Active", "|", "awk", "'{print $2}'"]
            reset_cmd = ["systemctl", "reset-failed",
                         "ceph-radosgw@rgw.%s" % (service)]
            cmd = ["systemctl", "restart",
                   "ceph-radosgw@rgw.%s" % (service)]
        else:
            check_cmd = ["ps", "-ef", "|", "grep", types, "|", "grep",
                         "id\\ %s" % service, "|grep", "-v", "grep", "|",
                         "awk", "'{print $2}'"]
            status_cmd = ["systemctl", "status",
                          "ceph-%s@%s" % (types, service), "|", "grep",
                          "Active", "|", "awk", "'{print $2}'"]
            reset_cmd = ["systemctl", "reset-failed",
                         "ceph-%s@%s" % (types, service)]
            cmd = ["systemctl", "restart", "ceph-%s@%s" % (types, service)]
        # 检查进程存在
        rc, pid, c_err = self.run_command(check_cmd, timeout=5)
        if not pid:
            # 服务未启动 检查状态
            rc, s_out, s_err = self.run_command(status_cmd, timeout=5)
            # 错误状态需要重置为 inactive 状态，才能启动
            if s_out.strip('\r\n') == "failed":
                self.run_command(reset_cmd, timeout=5)
        # 重启
        rc, stdout, stderr = self.run_command(cmd, timeout=35)
        while True:
            s_rc, s_out, s_err = self.run_command(status_cmd, timeout=5)
            if rc or s_out.strip('\r\n') != "active":
                if retrys == 0 or s_out.strip('\r\n') == "failed":
                    logger.error("Service ceph - {}@{} Status: % {}."
                                 "Restart Failed!".format(
                                     types, service, s_out, s_err))
                    raise SystemctlRestartError(service="ceph-{}@{}".format(
                        types, service), state=s_out)
                retrys = retrys - 1
            else:
                break
        rc, stdout, stderr = self.run_command(check_cmd, timeout=5)
        if stdout == pid:
            logger.error("Service ceph - {}@{} restart Failed!"
                         "The PID is the same as before".format(
                             types, service, s_out, s_err))
            raise SystemctlRestartError(
                service="ceph-{}@{}".format(types, service), state=s_out)
        return stdout

    def service_stop(self, types, service, retrys=12):
        logger.info("Try to stop service type %s, service %s", types, service)
        if types == "rgw":
            check_cmd = ["ps", "-ef", "|", "grep", "rgw", "|", "grep",
                         "client.rgw.%s" % service, "|grep", "-v", "grep"]
            stop_cmd = ["systemctl", "stop", "ceph-radosgw@rgw.%s" % (service)]
        else:
            # TODO add other service
            check_cmd = ["ps", "-ef", "|", "grep", "osd", "|", "grep", "--",
                         "'id %s '" % service]
            stop_cmd = ["systemctl", "stop", "ceph-%s@%s" % (types, service)]
        while True:
            # stop
            rc, stdout, stderr = self.run_command(stop_cmd, timeout=35)
            if rc == 1 and "canceled" in stderr:
                logger.warning("wait stop: code(%s), out(%s), err(%s)",
                               rc, stdout, stderr)
            elif rc:
                raise RunCommandError(cmd=stop_cmd, return_code=rc,
                                      stdout=stdout, stderr=stderr)
            # check
            rc, stdout, stderr = self.run_command(check_cmd, timeout=5)
            logger.debug("wait stop: code(%s), out(%s), err(%s)",
                         rc, stdout, stderr)
            if types in stdout:
                time.sleep(10)
                retrys = retrys - 1
                if retrys < 0:
                    raise ActionTimeoutError(reason="Stop %s %s" % (types,
                                                                    service))
            else:
                return True

    def slow_request_get(self, osds):
        res = []
        for osd in osds:
            if not osd.osd_id:
                continue
            logger.debug("Osd.{} slow request get start.".format(osd.osd_id))
            cmd = ["ceph", "daemon", "osd.%s" % osd.osd_id,
                   "dump_historic_slow_ops", '-f', 'json']
            rc, stdout, stderr = self.run_command(cmd, timeout=5)
            if rc == 22:
                logger.warning("osd.%s not found." % osd.osd_id)
                continue
            elif rc:
                logger.error("Command: %(cmd)s ReturnCode: %(return_code)s "
                             "Stderr: %(stderr)s Stdout: %(stdout)s.".format(
                                 cmd=cmd, return_code=rc,
                                 stdout=stdout, stderr=stderr
                             ))
                continue
            res.append({
                "id": osd.id,
                "osd_id": osd.osd_id,
                "osd_name": osd.osd_name,
                "node_id": osd.node_id,
                "hostname": osd.node.hostname,
                "ops": json.loads(encodeutils.safe_decode(stdout)).get("Ops")
            })
        return res

    def osd_stop(self, osd_id):
        # check command
        check_cmd = ["ps", "-ef", "|", "grep", "osd", "|", "grep", "--",
                     "'id %s '" % osd_id]
        retrys = 12
        while True:
            # stop
            cmd = ["systemctl", "stop", "ceph-osd@%s" % osd_id]
            rc, stdout, stderr = self.run_command(cmd, timeout=35)
            if rc == 1 and "canceled" in stderr:
                logger.warning("wait stop: code(%s), out(%s), err(%s)",
                               rc, stdout, stderr)
            elif rc:
                raise RunCommandError(cmd=cmd, return_code=rc,
                                      stdout=stdout, stderr=stderr)
            # check
            rc, stdout, stderr = self.run_command(check_cmd, timeout=5)
            logger.debug("wait stop: code(%s), out(%s), err(%s)",
                         rc, stdout, stderr)
            if "ceph-osd" in stdout:
                time.sleep(10)
                retrys = retrys - 1
                if retrys < 0:
                    raise ActionTimeoutError(reason="Stop osd.%s" % osd_id)
            else:
                return

    def osd_umount(self, osd_id):
        path = "/var/lib/ceph/osd/ceph-%s" % osd_id
        cmd = ["umount", path]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc == 32:
            return
        elif rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["rm", "-rf", path]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)

    def ceph_config_update(self, values):
        config_dir = self._wapper('/etc/ceph/')
        if not os.path.exists(config_dir):
            os.makedirs(config_dir, mode=0o0755)
        path = self._wapper('/etc/ceph/ceph.conf')
        configer = configparser.ConfigParser()
        configer.read(path)
        if not configer.has_section(values['group']):
            configer.add_section(values['group'])
        configer.set(values['group'], values['key'], str(values['value']))
        configer.write(open(path, 'w'))

    def ceph_config_remove(self, section, key):
        path = self._wapper('/etc/ceph/ceph.conf')
        configer = configparser.ConfigParser()
        configer.read(path)
        if not configer.has_section(section):
            logger.info('ceph config has no section %s', section)
            return
        configer.remove_option(section, key)
        configer.write(open(path, 'w'))

    def crushmap_compile(self, source=None):
        if source is None:
            source = DEFAULT_CURSH_PATH
        if not os.path.exists(source):
            raise CrushMapNotFound(path=source)
        output = '/tmp/crush_compiled'
        cmd = ["crushtool", "-c", source, "-o", output]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return output


def get_json_output(json_databuf):
    if len(json_databuf) == 0:
        return None
    outbuf = encodeutils.safe_decode(json_databuf)
    logger.debug('get_json_output, outbuf: {}'.format(outbuf))
    outdata = json.loads(outbuf)
    return outdata


class RBDProxy(object):
    def __init__(self, rados_client=None, pool_name=DEFAULT_POOL):
        self.rbd_inst = rbd.RBD()
        self.io_ctx = rados_client.get_io_ctx(pool_name)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.io_ctx.close()

    def __del__(self):
        self.io_ctx.close()

    def rbd_create(self, rbd_name, rbd_size):
        size = int(rbd_size)
        try:
            self.rbd_inst.create(self.io_ctx, rbd_name, size)
        except rbd.Error as e:
            logger.error("create rbd: {} error".format(rbd_name))
            raise CephException(message=str(e))

    def rbd_remove(self, rbd_name):
        try:
            self.rbd_inst.remove(self.io_ctx, rbd_name)
        except rbd.Error as e:
            logger.error("remove rbd: {} error".format(rbd_name))
            raise CephException(message=str(e))

    def rbd_clone(self, p_v_name, p_s_name, c_io_ctx, c_v_name):
        try:
            self.rbd_inst.clone(
                self.io_ctx, p_v_name, p_s_name, c_io_ctx, c_v_name)
        except rbd.Error as e:
            raise CephException(message=str(e))

    def rbd_rename(self, src, dest):
        try:
            self.rbd_inst.rename(self.io_ctx, src, dest)
        except rbd.Error as e:
            raise CephException(message=str(e))

    def rbd_list(self):
        """
        Return a list of rbd images
        """
        return self.rbd_inst.list(self.io_ctx)

    def rbd_size(self, rbd_name):
        try:
            image = rbd.Image(self.io_ctx, rbd_name)
            return image.size()
        except rbd.Error:
            return -1

    def rbd_snap_create(self, rbd_name, snap_name):
        try:
            image = rbd.Image(self.io_ctx, rbd_name)
            image.create_snap(snap_name)
        except Exception as e:
            raise CephException(message=str(e))

    def list_snaps(self, rbd_name):
        try:
            image = rbd.Image(self.io_ctx, rbd_name)
            return image.list_snaps()
        except rbd.Error:
            return None

    def rbd_enable_application(self, appname='rbd'):
        self.io_ctx.application_enable(appname)

    def rbd_snap_remove(self, rbd_name, snap_name):
        try:
            image = rbd.Image(self.io_ctx, rbd_name)
            image.remove_snap(snap_name)
        except rbd.Error as e:
            raise CephException(message=str(e))

    def rbd_snap_rename(self, rbd_name, old_name, new_name):
        try:
            image = rbd.Image(self.io_ctx, rbd_name)
            image.rename_snap(old_name, new_name)
        except Exception as e:
            raise CephException(message=str(e))

    def is_protect_snap(self, volume_name, snap_name):
        try:
            image = rbd.Image(self.io_ctx, volume_name)
            return image.is_protected_snap(snap_name)
        except Exception as e:
            raise CephException(message=str(e))

    def protect_snap(self, volume_name, snap_name):
        try:
            image = rbd.Image(self.io_ctx, volume_name)
            image.protect_snap(snap_name)
        except Exception as e:
            raise CephException(message=str(e))

    def rbd_unprotect_snap(self, volume_name, snap_name):
        try:
            image = rbd.Image(self.io_ctx, volume_name)
            image.unprotect_snap(snap_name)
        except Exception as e:
            raise CephException(message=str(e))

    def rbd_image_flatten(self, c_v_name):
        try:
            image = rbd.Image(self.io_ctx, c_v_name)
            image.flatten()
        except Exception as e:
            raise CephException(message=str(e))

    def rbd_rollback_to_snap(self, v_name, s_name):
        try:
            image = rbd.Image(self.io_ctx, v_name)
            image.rollback_to_snap(s_name)
        except Exception as e:
            raise CephException(message=str(e))

    def rbd_resize(self, v_name, size):
        try:
            image = rbd.Image(self.io_ctx, v_name)
            image.resize(size)
        except Exception as e:
            raise CephException(message=str(e))


class RADOSClient(object):
    """Context manager to simplify error handling for connecting to ceph."""

    def __init__(self, ceph_conf, timeout=None):
        self.client = rados.Rados(conf=ceph_conf)
        if timeout:
            timeout = str(timeout)
            self.client.conf_set('rados_osd_op_timeout', timeout)
            self.client.conf_set('rados_mon_op_timeout', timeout)
            self.client.conf_set('client_mount_timeout', timeout)
        try:
            self.client.connect()
        except rados.TimedOut:
            raise CephConnectTimeout()

    def __enter__(self):
        return self

    def __del__(self):
        self.client.shutdown()

    def __exit__(self, type_, value, traceback):
        self.client.shutdown()

    def _disconnect_from_rados(self, client):
        client.shutdown()

    def get_io_ctx(self, pool_name):
        return self.client.open_ioctx(pool_name)

    def get_mon_status(self):
        ret, mon_dump_outbuf, __ = self.client.mon_command(
            '{"prefix":"mon dump", "format":"json"}', '')
        if ret:
            return None
        mon_dump_data = get_json_output(mon_dump_outbuf)
        epoch = mon_dump_data['epoch']
        fsid = mon_dump_data['fsid']
        mons = mon_dump_data['mons']
        # Return overall stats
        return {
            "fsid": fsid,
            "epoch": epoch,
            "mons": mons
        }

    # ceph df ==> ceph & pool size
    def get_ceph_df(self):
        ret, df_outbuf, __ = self.client.mon_command(
            '{"prefix":"df", "format":"json"}', '')
        if ret:
            return None
        df_outbuf = encodeutils.safe_decode(df_outbuf)
        df_data = json.loads(df_outbuf)
        return df_data

    # ceph osd df ==> osd size
    def get_osd_df(self):
        ret, df_outbuf, __ = self.client.mon_command(
            '{"prefix":"osd df", "format":"json"}', '')
        if ret:
            return None
        df_outbuf = encodeutils.safe_decode(df_outbuf)
        df_data = json.loads(df_outbuf)
        return df_data

    # ceph df ==> cluster and pool_list size
    def get_cluster_info(self):
        fsid = self.client.get_fsid()
        fsid = encodeutils.safe_decode(fsid)
        ret, df_outbuf, __ = self.client.mon_command(
            '{"prefix":"df", "format":"json"}', '')
        if ret:
            return None
        df_outbuf = encodeutils.safe_decode(df_outbuf)
        df_data = json.loads(df_outbuf)
        # Return fsid, cluster_data
        return {
            "fsid": fsid,
            "cluster_data": df_data
        }

    # Get pool replica size
    def get_pool_stats(self, pool_name=DEFAULT_POOL):
        command_str = '{"var": "all", "prefix": "osd pool get", "pool": \
            "%(pool)s", "format":"json"}' % {'pool': pool_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, pool_stats_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            return None
        pool_stats_outdata = get_json_output(pool_stats_outbuf)
        return {
            "pool": pool_stats_outdata['pool'],
            "pool_id": pool_stats_outdata['pool_id'],
            "size": pool_stats_outdata['size'],
            "min_size": pool_stats_outdata['min_size'],
            "pg_num": pool_stats_outdata['pg_num'],
            "pgp_num": pool_stats_outdata['pgp_num'],
            "crush_rule": pool_stats_outdata['crush_rule']
        }

    def pool_list(self):
        """
        Get all pools list in ceph cluster
        """
        return self.client.list_pools()

    def pool_exists(self, pool):
        return self.client.pool_exists(pool)

    def _ec_pool_create(self, pg_num, rule_name, pool_name):
        cmd_ec = {
            "pool_type": "erasure",
            "prefix": "osd pool create",
            "pg_num": pg_num,
            "erasure_code_profile": rule_name + ECP_NAME_END,
            "pool": pool_name,
            "rule": rule_name,
        }
        self._send_mon_command(json.dumps(cmd_ec))

    def _ec_pool_set_true(self, pool_name):
        cmd_set_true = {
            "var": "allow_ec_overwrites",
            "prefix": "osd pool set",
            "pool": pool_name,
            "val": "true"

        }
        self._send_mon_command(json.dumps(cmd_set_true))

    def _erasure_pool_create(self, pool_name, rule_name, pg_num,
                             pgp_num=None, rep_size=None):
        """
        1. 创建ec池，2. set true, 3. 创建一个副本池
        ceph osd pool create ec-pool 512 erasure ecp-2-1 erasure_2-1
        ceph osd pool set ec-pool allow_ec_overwrites true
        ceph osd pool create rep-pool 512
        """
        self._ec_pool_create(pg_num, rule_name, pool_name)
        self._ec_pool_set_true(pool_name)
        # create a replicated_pool
        re_pool_name = pool_name + EC_POOL_RELATION_RE_POOL
        re_rule_name = rule_name + EC_POOL_RELATION_RE_POOL
        pg_num = pgp_num = CONF.erasure_default_pg_num
        self._replicated_pool_create(
            re_pool_name, pool_type='replicated', rule_name=re_rule_name,
            pg_num=pg_num, pgp_num=pgp_num, rep_size=rep_size)

    def pool_set_min_size(self, pool_name=DEFAULT_POOL, min_size=None):
        return self.set_pool_info(pool_name, "min_size", min_size)

    def pool_set_replica_size(self, pool_name=DEFAULT_POOL, rep_size=None):
        return self.set_pool_info(pool_name, "size", rep_size)

    def pool_create(self, pool_name=None, pool_type=None, rule_name=None,
                    ec_profile=None, pg_num=None, pgp_num=None, rep_size=None):
        """
        Create replicated pool or EC pool
        ceph osd pool create rbd 128 128 replicated replicated_rule 0 0 1
        ceph osd pool create rbd 128 128 erasure default
        """
        if pool_type == 'erasure':
            # TODO if ec_profile is none, will use a default ec profile
            # osd_pool_default_erasure_code_profile
            self._erasure_pool_create(pool_name, rule_name,
                                      pg_num, pgp_num, rep_size)
        else:   # replicated
            self._replicated_pool_create(pool_name, pool_type, rule_name,
                                         pg_num, pgp_num, rep_size)

    def _replicated_pool_create(self, pool_name=DEFAULT_POOL,
                                pool_type='replicated',
                                rule_name='replicated_rule',
                                pg_num=None, pgp_num=None, rep_size=None):
        """
        osd pool create <poolname> <int[0-]> {<int[0-]>} {replicated|erasure} \
            {<erasure_code_profile>} {<rule>} {<int>} :  create pool
        """
        cmd = {
            "rule": "0",
            "pool_type": pool_type,
            "prefix": "osd pool create",
            "pg_num": pg_num,
            "erasure_code_profile": rule_name,
            "pgp_num": pgp_num,
            "expected_num_objects": 0,
            "format": "json",
            "pool": pool_name,
        }
        if rep_size:
            cmd["size"] = rep_size
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def config_set(self, service, key, value, osd_list=None):
        cmd = {
            "prefix": "config set",
            "key": key,
            "value": value
        }
        command_str = json.dumps(cmd)
        if service.startswith('osd'):
            if not osd_list:
                try:
                    self._send_osd_command(int(service.split('.')[1]),
                                           command_str)
                except CephException:
                    pass
            else:
                for osd in osd_list:
                    try:
                        self._send_osd_command(int(osd.osd_id), command_str)
                    except CephException:
                        pass
        if service.startswith('mon'):
            try:
                self._send_mon_command(command_str)
            except CephException:
                pass

    def conf_get(self, key):
        return self.client.conf_get(key)

    def conf_set(self, key, val):
        return self.client.conf_set(key, val)

    def _pool_delete(self, pool_name):
        return self.client.delete_pool(pool_name)

    def pool_delete(self, pool_name, pool_type=None):
        if pool_type == PoolType.REPLICATED:
            # 删除副本池： 1. del 副本池
            if self.pool_exists(pool_name):
                self.client.delete_pool(pool_name)
            else:
                logger.warning("pool %s not exists, ignore it", pool_name)
                return
        elif pool_type == PoolType.ERASURE:
            # 删除纠删码池：1. del 纠删码池，2. del 附属的副本池
            extra_re_pool = pool_name + EC_POOL_RELATION_RE_POOL
            if self.pool_exists(pool_name):
                self.client.delete_pool(pool_name)
            else:
                logger.warning("pool %s not exists, ignore it", pool_name)
            if self.pool_exists(extra_re_pool):
                self.client.delete_pool(extra_re_pool)
            else:
                logger.warning("pool %s not exists, ignore it", extra_re_pool)

    def osd_df_list(self, pool_name=DEFAULT_POOL):
        pass

    def osd_list(self, host_name=None, rack_name=None, pool_name=None):
        pass

    def bucket_remove(self, bucket, ancestor=None):
        """
        Remove a bucket
        {"prefix": "osd crush remove", "name": "da2"}

        Remove a bucket from it's ancestor
        {
            "prefix": "osd crush remove",
            "ancestor": "ceph-3",
            "name": "osd.3"
        }
        """
        cmd = {
            "prefix": "osd crush remove",
            "name": bucket
        }
        if ancestor:
            cmd["ancestor"] = ancestor
        command_str = json.dumps(cmd)
        logger.info('bucket remove command_str: %s', command_str)
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("remove bucket {} error: {}".format(
                bucket, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def bucket_get(self, bucket):
        """
        Get a bucket's childrent

        {
            "node": "pool-643b97a3e9d54fd3b0e1178bce1cce25-rack15",
            "prefix": "osd crush ls",
            "format": "json"
        }
        """
        cmd = {
            "node": bucket,
            "prefix": "osd crush ls",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        bucket_info = self._send_mon_command(command_str)
        return bucket_info

    def _bucket_move(self, bucket_name, ancestor_type, ancestor_name):
        """
        {
            "prefix": "osd crush move",
            "args": "[<ancestor_type>=<ancestor_name>]",
            "name": "<bucket_name>"
        }
        """
        cmd = {
            "prefix": "osd crush move",
            "args": [
                "{}={}".format(ancestor_type, ancestor_name)
            ],
            "name": bucket_name
        }
        command_str = json.dumps(cmd)
        logger.info('bucket move, command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("bcuket move, from %s to %s error: %s", bucket_name,
                         ancestor_name, mon_dump_outbuf)
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def datacenter_move_to_root(self, datacenter_name, root_name):
        self._bucket_move(datacenter_name, "root", root_name)

    def rack_move_to_root(self, rack_name, root_name):
        self._bucket_move(rack_name, "root", root_name)

    def rack_move_to_datacenter(self, rack_name, datacenter_name):
        self._bucket_move(rack_name, "datacenter", datacenter_name)

    def host_move_to_root(self, host_name, root_name):
        self._bucket_move(host_name, "root", root_name)

    def host_move_to_rack(self, host_name, rack_name):
        self._bucket_move(host_name, "rack", rack_name)

    def osd_add(self, osd_id, osd_size, host_name):
        """
        Add a osd with specified weight to a specified host
        ceph osd crush add osd.1 0.0976 host=ceph-3
        {"prefix": "osd crush add", "args": ["host=iscsi-t1"], \
         "id": 1, "weight": 0.0976}
        """
        weight = float(osd_size) / (2**40)
        cmd = {
            "format": "json",
            "prefix": "osd crush add",
            "args": ["host=%s" % host_name],
            "id": int(osd_id.replace("osd.", "")),
            "weight": weight
        }
        command_str = json.dumps(cmd)
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, err = self.client.mon_command(command_str, '')
        if ret:
            msg = "add osd.{} to {} error: code({}) out({}) err({})".format(
                osd_id, host_name, ret, mon_dump_outbuf, err)
            logger.error(msg)
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def get_ceph_report(self):
        """
        Get ceph report
        :return:
        """
        cmd = {
            "format": "json",
            "prefix": "report"
        }
        command_str = json.dumps(cmd)
        logger.info('command_str: {}'.format(command_str))
        report_info = self._send_mon_command(command_str)
        logger.debug("Ceph report: %s", report_info)
        return report_info

    def get_crushmap(self):
        """
        Get crushmap json
        :return: crushmap
        """
        logger.info("Get crush map")
        cmd = {
            "prefix": "osd crush dump"
        }
        command_str = json.dumps(cmd)
        logger.info('command_str: {}'.format(command_str))
        crushmap = self._send_mon_command(command_str)
        logger.debug("Ceph crushmap: %s", crushmap)
        return crushmap

    def set_crushmap(self, map, source=None):
        """
        Set crushmap to ceph
        :param map: json type of crushmap
        :return: None
        """
        logger.info("Set cursh map: %s, %s", map, source)
        if source is None:
            source = DEFAULT_CURSH_PATH
        # Convert map from json to text
        self.convert_crushmap(map, source)
        # Complie crushmap text
        ceph_tool = CephTool(Executor())
        dest = ceph_tool.crushmap_compile(source)
        # Set crushmap
        cmd = {
            "prefix": "osd setcrushmap"
        }
        command_str = json.dumps(cmd)
        logger.info('command_str: {}'.format(command_str))
        with open(dest, "rb") as file:
            inbuf = file.read()
        self._send_mon_command(command_str, inbuf)

    def convert_crushmap(self, map, path=None):
        """
        Convert crushmap from json to normal format
        :param map: json crushmap
        :return:
        """
        logger.info("Convert crush map %s", map)
        if path is None:
            path = DEFAULT_CURSH_PATH
        if os.path.exists(path):
            os.remove(path)
        with open(path, "a") as file:
            # tunables
            tunables = map.get("tunables")
            for k, v in six.iteritems(tunables):
                if k not in SUPPORTED_TUNABLES:
                    continue
                file.write("tunable {} {}\n".format(k, v))
            file.write("\n")
            # devices
            devices = map.get("devices")
            for device in devices:
                if device["name"].startswith("device"):
                    device["name"] = "osd.{}".format(device["id"])
                device_str = "device {} {}".format(
                    device["id"], device["name"])
                if device.get("class"):
                    device_str += " class {}\n".format(device["class"])
                else:
                    device_str += "\n"
                file.write(device_str)
            file.write("\n")
            # types
            types = map.get("types")
            for t in types:
                file.write("type {} {}\n".format(t["type_id"], t["name"]))
            file.write("\n")
            # buckets
            buckets_old = map.get("buckets")
            buckets_new = {}
            iname_map = {}
            # combine buckets end with ~hdd or ~ssd
            for bucket in buckets_old:
                bucket_name = bucket["name"]
                bucket_class = ""
                iname_map[bucket["id"]] = bucket["name"]
                if "~" in bucket["name"]:
                    tmp_name = bucket_name.split("~")
                    bucket_name = tmp_name[0]
                    bucket_class = tmp_name[1]

                if buckets_new.get(bucket_name):
                    # buckets_new[bucket_name]["bucket_class"] = bucket_class
                    buckets_new[bucket_name]["class_id"] = bucket["id"]
                    if not bucket_class:
                        buckets_new[bucket_name]["id"] = bucket["id"]
                        buckets_new[bucket_name]["items"] = bucket["items"]
                else:
                    buckets_new[bucket_name] = bucket
                    buckets_new[bucket_name]["name"] = bucket_name
            logger.debug("Buckets combined: %s", buckets_new)
            file.write("\n")
            # write buckets
            tail_buckets = []
            datacenter_bucket = {
                "rack": [],
                "datacenter": [],
                "root": []
            }
            for key, bucket in six.iteritems(buckets_new):
                tail = False
                root = False
                seq = [
                    ("%s %s {\n" % (bucket["type_name"], bucket["name"])),
                    "id {}\n".format(bucket["id"])
                ]
                # if bucket.get("class_id"):
                #     seq.append("id {} class {}\n".format(
                #         bucket["class_id"], bucket["bucket_class"]))
                seq = seq + [
                    "alg {}\n".format(bucket["alg"]),
                    "hash {}\n".format(bucket["hash"])
                ]
                items = bucket.get("items")
                if not items:
                    seq.append("}\n")
                    file.writelines(seq)
                    continue
                for i in items:
                    if int(i["id"]) < 0:
                        item_name = iname_map[i["id"]]
                        # Need to be written after items
                        tail = True
                        if "datacenter" in item_name or "rack" in item_name:
                            root = True
                    else:
                        item_name = "osd.{}".format(i["id"])
                    seq.append("item {} weight {}\n".format(
                        item_name, format(i["weight"]/65536, ".5f")))
                seq.append("}\n")
                if bucket["type_name"] == "rack":
                    datacenter_bucket["rack"].append(seq)
                    continue
                if bucket["type_name"] == "datacenter":
                    datacenter_bucket["datacenter"].append(seq)
                    continue
                if root:
                    datacenter_bucket["root"].append((seq))
                    continue
                if tail:
                    tail_buckets.append(seq)
                    continue
                file.writelines(seq)
            for b in tail_buckets:
                file.writelines(b)
            for bucket_type, buckets in six.iteritems(datacenter_bucket):
                for bucket in buckets:
                    file.writelines(bucket)
            file.write("\n")
            # rules
            rules = map.get("rules")
            for rule in rules:
                seq = [
                    ("rule %s {\n" % rule["rule_name"]),
                    "id {}\n".format(rule["rule_id"]),
                    "type {}\n".format(RULE_TYPE_MAP[rule["type"]]),
                    "min_size {}\n".format(rule["min_size"]),
                    "max_size {}\n".format(rule["max_size"]),
                ]
                for step in rule.get("steps"):
                    if step["op"] == "set_chooseleaf_tries":
                        seq.append("step set_chooseleaf_tries {}\n".format(
                            step["num"]))
                    if step["op"] == "set_choose_tries":
                        seq.append("step set_choose_tries {}\n".format(
                            step["num"]))
                    if step["op"] == "take":
                        seq.append("step take {}\n".format(step["item_name"]))
                    # fault domain is not osd
                    # rep pool
                    if step["op"] == "chooseleaf_firstn":
                        seq.append(
                            "step chooseleaf firstn {} type {}\n".format(
                                step["num"], step["type"]))
                    # erasure pool
                    if step["op"] == "chooseleaf_indep":
                        seq.append(
                            "step chooseleaf indep {} type {}\n".format(
                                step["num"], step["type"]))
                    # fault domain is osd
                    # rep pool
                    if step["op"] == "choose_firstn":
                        seq.append(
                            "step choose firstn {} type {}\n".format(
                                step["num"], step["type"]))
                    # erasure pool
                    if step["op"] == "choose_indep":
                        seq.append(
                            "step choose indep {} type {}\n".format(
                                step["num"], step["type"]))

                    if step["op"] == "emit":
                        seq.append("step emit\n")
                seq.append("}\n")
                file.writelines(seq)

    def _bucket_add(self, bucket_type, bucket_name):
        """
        {
            "prefix": "osd crush add-bucket",
            "type": "<bucket type>",
            "name": "<bucket name>"
        }
        """
        cmd = {
            "prefix": "osd crush add-bucket",
            "type": bucket_type,
            "name": bucket_name
        }
        command_str = json.dumps(cmd)
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("bcuket add %s %s error: %s", bucket_type,
                         bucket_name, mon_dump_outbuf)
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def host_add(self, host_name):
        self._bucket_add("host", host_name)

    def rack_add(self, rack_name):
        self._bucket_add("rack", rack_name)

    def datacenter_add(self, datacenter_name):
        self._bucket_add("datacenter", datacenter_name)

    def root_add(self, root_name):
        self._bucket_add("root", root_name)

    def _send_mon_command(self, command_str, inbuf=""):
        ret, outbuf, err = self.client.mon_command(command_str, inbuf)
        if ret == -110:
            raise CephCommandTimeout()
        if ret:
            raise CephException(
                message="execute command failed: {}, ret: {}, outbuf: {}, "
                "err: {}".format(command_str, ret, outbuf, err))
        return get_json_output(outbuf)

    def _send_osd_command(self, osd_id, command_str):
        if isinstance(osd_id, six.string_types):
            osd_id = int(osd_id)
        if not isinstance(osd_id, int):
            raise CephException(_("osd id must int type"))
        ret, outbuf, err = self.client.osd_command(osd_id, command_str, '')
        if ret:
            raise CephException(
                message="execute command failed: {}, ret: {}, outbuf: {}, "
                "err: {}".format(command_str, ret, outbuf, err))
        return get_json_output(outbuf)

    def get_device_class(self):
        """
        {"prefix": "osd crush class ls", "format": "json"}
        """
        cmd = {
            "prefix": "osd crush class ls",
            "format": "json",
        }
        command_str = json.dumps(cmd)
        device_class_data = self._send_mon_command(command_str)
        return device_class_data

    # TODO
    def set_scrub_time(self, begin, end):
        pass

    def get_pool_info(self, pool_name, keyword):
        cmd = {
            "var": keyword,
            "prefix": "osd pool get",
            "pool": pool_name,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        pool_info = self._send_mon_command(command_str)
        return pool_info

    def get_pool_pgs(self, pool_name):
        command_str = '{"prefix": "pg ls-by-pool", "poolstr": "rbd", "target":'
        '["mgr",""], "format": "json"}'
        pool_pgs_info = self._send_mon_command(command_str)
        return pool_pgs_info

    def get_osds_by_pool(self, pool_name):
        pool_pg_data = self.get_pool_pgs(pool_name)
        osds = []
        for pg in pool_pg_data:
            for osd in pg.get('acting'):
                if osd not in osds:
                    osds.append(osd)

    def get_osds_by_bucket(self, bucket_name):
        command_str = '{"prefix": "osd ls-tree", "name": "%(bucket)s", \
                "format": "json"}' % {'bucket': bucket_name}
        osds = self._send_mon_command(command_str)
        return osds

    def set_pool_info(self, pool_name, keyword, value):
        cmd = {
            "var": keyword,
            "prefix": "osd pool set",
            "pool": pool_name,
            "format": "json",
            "val": value
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def rule_remove(self, rule_name):
        """
        ceph osd crush rule rm <rule_name>
        """
        cmd = {
            "prefix": "osd crush rule rm",
            "format": "json",
            "name": rule_name
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def create_ec_profile(self, ecp_name, choose_type, extra_data, root_name):
        k, m = extra_data['k'], extra_data['m']
        profile = ["k=%s" % k, "m=%s" % m, "crush-failure-domain=%s" %
                   choose_type, "crush-root=%s" % root_name]
        cmd_es_profile = {
            "profile": profile,
            "prefix": "osd erasure-code-profile set",
            "name": ecp_name}
        cmd_ecp = json.dumps(cmd_es_profile)
        self._send_mon_command(cmd_ecp)

    def create_replicated_rule(self, cmd_re_rule):
        self._send_mon_command(json.dumps(cmd_re_rule))

    def create_erasure_rule(self, rule_name, choose_type, extra_data,
                            root_name, cmd_re_rule):
        """
        1.create es_profile 2.create erasure_rule 3.create replicated_rule
        后台创建两个crush_rule(ec_rule, 副本_rule)
        1. ceph osd erasure-code-profile set ecp-2-1 k=2 m=1
        crush-failure-domain=host
        2. ceph osd crush rule create-erasure erasure_2-1 ecp-2-1
        when create erasure rule, ecp name ==
        crush_rule_name + ECP_NAME_END
        3. create replicated_rule
        replicated_rule_name = crush_rule_name + EC_POOL_RELATION_RE_POOL
        """
        ecp_name = rule_name + ECP_NAME_END
        self.create_ec_profile(ecp_name, choose_type, extra_data,
                               root_name)
        cmd_rule = {
            "root": root_name,
            "profile": ecp_name,
            "prefix": "osd crush rule create-erasure",
            "type": choose_type,
            "name": rule_name}
        self._send_mon_command(json.dumps(cmd_rule))
        # create replicated rule
        cmd_re_rule.update({'name': rule_name + EC_POOL_RELATION_RE_POOL})
        self.create_replicated_rule(cmd_re_rule)

    def rule_add(self, rule_name='replicated_rule', root_name='default',
                 choose_type='host', device_class=None, rule_type=None,
                 extra_data=None):
        cmd_re_rule = {
            "root": root_name,
            "prefix": "osd crush rule create-replicated",
            "type": choose_type,
            "name": rule_name,
        }
        if device_class:
            cmd_re_rule["class"] = device_class
        if rule_type == PoolType.REPLICATED:
            self.create_replicated_rule(cmd_re_rule)
        elif rule_type == PoolType.ERASURE:
            self.create_erasure_rule(rule_name, choose_type, extra_data,
                                     root_name, cmd_re_rule)

    def rule_get(self, rule_name='replicated_rule'):
        cmd = {
            "prefix": "osd crush rule dump",
            "name": rule_name,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)
        rule_detail = self._send_mon_command(command_str)
        return rule_detail

    def rule_rename(self, srcname='replicated_rule', dstname=None):
        if not dstname:
            return
        cmd = {
            "srcname": srcname,
            "prefix": "osd crush rule rename",
            "dstname": dstname
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def get_osd_hosts(self):
        logger.debug("detect osds from cluster")

        cmd = {
            "prefix": "osd dump",
            "format": "json",
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
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

    def get_mgr_hosts(self):
        logger.debug("detect active mgr from cluster")

        cmd = {
            "prefix": "mgr dump",
            "format": "json",
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        mgr_addr = res.get('active_addr')
        if not mgr_addr or mgr_addr == '-':
            return None

        mgr_addr = mgr_addr.split(':')[0]
        mgr_hosts = []
        mgr_hosts.append(mgr_addr)

        return mgr_hosts

    def get_mon_hosts(self):
        logger.debug("detect mons from cluster")

        cmd = {
            "prefix": "mon dump",
            "format": "json",
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        mons = res.get('mons')
        if not mons:
            return None

        mon_hosts = []
        for mon in mons:
            public_addr = mon.get('public_addr')
            public_addr = public_addr.split(':')[0]
            mon_hosts.append(public_addr)

        return mon_hosts

    def set_balance_mode(self, balance_mode):
        logger.debug("set balance mode to: %s", balance_mode)
        cmd = {
            "prefix": "balancer mode",
            "mode": balance_mode,
            "target": [
                "mgr", ""
            ]
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("set balancer status success %s", res)
        return res

    def set_balance_status(self, action):
        logger.info("set balancer: %s", action)
        cmd = {
            "prefix": "balancer {}".format(action),
            "target": [
                "mgr", ""
            ]
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("set balancer status success %s", res)
        return res

    def set_min_compat_version(self, version):
        logger.debug("set require min compat client version to: %s", version)
        cmd = {
            "prefix": "osd set-require-min-compat-client",
            "version": version
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("get all pools: %s", res)
        return res

    @retry(RunCommandError)
    def data_balancer_available(self):
        cmd = {
            "prefix": "balancer status",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        ret, outbuf, err = self.client.mon_command(command_str, '')
        # can't get balancer status if we enable module just now
        if ret == -110:
            logger.error("can't connect to cluster")
            return False
        elif ret == -22:
            logger.error("can't get balancer status now")
            raise RunCommandError(cmd=command_str, return_code=ret,
                                  stdout=outbuf, stderr=err)
        elif ret == 0:
            return True

    def set_data_balance(self, action, mode):
        # always enable balancer when setting balance on/off
        res = self.mgr_module_ls()
        if "balancer" not in res["enabled_modules"]:
            self.mgr_module_enable("balancer")
        if not self.data_balancer_available():
            st_data = {
                "active": False,
                "mode": "none"
            }
            return st_data

        if mode == 'upmap':
            self.set_min_compat_version(REQUIRED_VERSION)
        self.set_balance_mode(mode)
        self.set_balance_status(action)
        st_data = self.balancer_status()
        return st_data

    def osd_new(self, osd_fsid):
        logger.debug("detect mons from cluster")

        cmd = {
            "prefix": "osd new",
            "uuid": osd_fsid,
            "format": "json",
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res.get("osdid")

    def get_pools(self):
        """
        {"prefix": "osd lspools", "format": "json"}
        """
        cmd = {
            "prefix": "osd lspools",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("get all pools: %s", res)
        return res

    def get_crush_rule_info(self, rule_name):
        logger.info("get info for crush rule: %s", rule_name)
        cmd = {
            "prefix": "osd crush rule dump",
            "name": rule_name,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("crush rule %s, info: %s", rule_name, res)
        return res

    def set_pool_application(self, pool_name, app_name):
        """
        {"prefix": "osd pool application enable", "app": "rbd", "pool": "rbd"}
        """
        logger.info("set pool %s application to %s", pool_name, app_name)
        cmd = {
            "prefix": "osd pool application enable",
            "app": app_name,
            "pool": pool_name
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def crush_tree(self):
        logger.info("osd crush tree")
        cmd = {
            "prefix": "osd crush tree",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osd_pause(self):
        logger.info("osd pause")
        cmd = {
            "prefix": "osd pause",
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def osd_unpause(self):
        logger.info("osd unpause")
        cmd = {
            "prefix": "osd unpause",
        }
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

    def status(self):
        logger.info("status")
        cmd = {
            "prefix": "status",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osd_out(self, osd_names):
        logger.info("osd out %s", osd_names)
        if not isinstance(osd_names, list):
            osd_names = [osd_names]
        cmd = {
            "prefix": "osd out",
            "ids": osd_names,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osd_down(self, osd_names):
        logger.info("osd down %s", osd_names)
        if not isinstance(osd_names, list):
            osd_names = [osd_names]
        cmd = {
            "prefix": "osd down",
            "ids": osd_names,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osd_crush_rm(self, osd_name):
        logger.info("osd crush rm %s", osd_name)
        cmd = {
            "prefix": "osd crush rm",
            "name": osd_name,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osd_rm(self, osd_names):
        logger.info("osd rm %s", osd_names)
        if not isinstance(osd_names, list):
            osd_names = [osd_names]
        cmd = {
            "prefix": "osd rm",
            "ids": osd_names,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def auth_del(self, osd_name):
        logger.info("auth del %s", osd_name)
        cmd = {
            "prefix": "auth del",
            "entity": osd_name,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osds_add_noout(self, osd_names):
        logger.info("osds add noout %s", osd_names)
        cmd = {
            "prefix": "osd add-noout",
            "ids": osd_names,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def osds_rm_noout(self, osd_names):
        logger.info("osds rm noout %s", osd_names)
        cmd = {
            "prefix": "osd rm-noout",
            "ids": osd_names,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        return res

    def mgr_module_ls(self):
        logger.info("mgr module ls")
        cmd = {
            "prefix": "mgr module ls",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("mgr model ls res: %s", res)
        return res

    def mgr_module_enable(self, module_name):
        logger.info("mgr module enable: %s", module_name)
        cmd = {
            "prefix": "mgr module enable",
            "module": module_name
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("mgr model enable res: %s", res)
        return res

    def balancer_status(self):
        logger.info("balancer status")
        cmd = {
            "prefix": "balancer status",
            "format": "json"
        }
        command_str = json.dumps(cmd)
        try:
            # Get balancer status failed when we enable balancer module_enable
            # just now, and failed when timedout or other reason.
            res = self._send_mon_command(command_str)
        except CephException as e:
            logger.error(e)
            res = {
                "active": False,
                "mode": "none"
            }
        logger.info("balancer status res: %s", res)
        return res

    def get_osd_tree(self):
        ret, outbuf, __ = self.client.mon_command(
            '{"prefix":"osd tree", "format":"json"}', '')
        if ret:
            return None
        outbuf = encodeutils.safe_decode(outbuf)
        df_data = json.loads(outbuf)
        return df_data

    def get_osd_stat(self):
        ret, outbuf, __ = self.client.mon_command(
            '{"prefix":"osd stat", "format":"json"}', '')
        if ret:
            return None
        outbuf = encodeutils.safe_decode(outbuf)
        df_data = json.loads(outbuf)
        return df_data

    def osd_metadata(self, osd_id):
        logger.info("get osd metadata: osd.%s", osd_id)
        if isinstance(osd_id, six.string_types):
            osd_id = int(osd_id)
        if not isinstance(osd_id, int):
            raise CephException(_("osd id must int type"))
        cmd = {
            "prefix": "osd metadata",
            "id": osd_id,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("get osd metadata res: %s", res)
        return res

    def auth_get_key(self, entity):
        logger.info("auth get: %s", entity)
        cmd = {
            "prefix": "auth get-key",
            "entity": entity,
            "format": "json"
        }
        command_str = json.dumps(cmd)
        res = self._send_mon_command(command_str)
        logger.info("auth get %s res: %s", entity, res)
        return res
