#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import json
import logging
import time

from oslo_utils import encodeutils

from DSpace.exception import ActionTimeoutError
from DSpace.exception import CephException
from DSpace.exception import RunCommandError
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


class CephTool(ToolBase):
    """Deprecated Ceph Tool

    All function need move to rados client
    """
    def get_networks(self):
        logger.debug("detect cluster networks")

        cmd = "ceph-conf --lookup cluster_network"
        rc, stdout, stderr = self.run_command(cmd, timeout=1)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cluster_network = stdout

        cmd = "ceph-conf --lookup public_network"
        rc, stdout, stderr = self.run_command(cmd, timeout=1)
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
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def module_enable(self, module):
        cmd = ["ceph", "mgr", "module", "enable", module]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def mon_install(self, hostname, fsid, ceph_auth='none'):

        if ceph_auth == 'cephx':
            # do cephx initial
            pass
        else:
            cmd = ["ceph-mon", "--cluster", "ceph", "--setuser",
                   "ceph", "--setgroup", "ceph", "--mkfs", "-i",
                   hostname, "--fsid", fsid]
            rc, stdout, stderr = self.run_command(cmd, timeout=60)
            if rc:
                raise RunCommandError(cmd=cmd, return_code=rc,
                                      stdout=stdout, stderr=stderr)

    def mon_uninstall(self, monitor_name):
        cmd = ['ceph', 'mon', 'remove', monitor_name]
        rc, stdout, stderr = self.run_command(cmd, timeout=60)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def disk_clear_partition_table(self, diskname):
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
        cmd = ["dspace-disk", "-v", "--log-stdout", "--setuser", "ceph",
               "--setgroup", "ceph",
               "prepare", ]
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
        logger.debug("stdout(%s), stderr(%s)", stdout, stderr)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    @retry(RunCommandError)
    def disk_active(self, diskname):
        cmd = ["dspace-disk", "activate", "/dev/%s1" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_deactivate(self, diskname):
        cmd = ["dspace-disk", "deactivate", "/dev/%s1" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_zap(self, diskname):
        cmd = ["dspace-disk", "zap", "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return True

    def osd_mark_out(self, osd_id):
        cmd = ["ceph", "osd", "down", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_remove_active(self, osd_id):
        cmd = ["ceph", "osd", "down", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_deactivate_flag(self, osd_id):
        mount = "/var/lib/ceph/osd/ceph-%s/" % osd_id
        for f in ['active', 'ready']:
            cmd = ["rm", "-rf", mount + f]
            rc, stdout, stderr = self.run_command(cmd, timeout=5)
        flag = mount + "deactive"
        cmd = ["su", "-", "ceph", "-s", "/bin/bash"  "-c", "'touch %s'" % flag]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc == 1 and "No such file" in stderr:
            return
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_stop(self, osd_id):
        cmd = ["systemctl", "disable", "ceph-osd@%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc == 1 and "No such file" in stderr:
            return
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
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
        if rc == 32 and "not found" in stderr:
            return
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["rm", "-rf", path]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)

    def osd_remove_from_cluster(self, osd_id):
        cmd = ["ceph", "osd", "down", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["ceph", "osd", "out", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["ceph", "osd", "crush", "remove", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["ceph", "osd", "rm", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        cmd = ["ceph", "auth", "del", "osd.%s" % osd_id]
        rc, stdout, stderr = self.run_command(cmd, timeout=5)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return True

    def ceph_config_update(self, values):
        path = self._wapper('/etc/ceph/ceph.conf')
        configer = configparser.ConfigParser()
        configer.read(path)
        if not configer.has_section(values['group']):
            configer.add_section(values['group'])
        configer.set(values['group'], values['key'], str(values['value']))
        configer.write(open(path, 'w'))


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
            raise CephException(message=e)

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
            self.client.conf_set('rados_osd_op_timeout', timeout)
            self.client.conf_set('rados_mon_op_timeout', timeout)
            self.client.conf_set('client_mount_timeout', timeout)
        self.client.connect()

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

    # TODO
    def _ec_pool_create(self):
        pass

    # TODO
    def pool_set_min_size(self, pool_name=DEFAULT_POOL, min_size=None):
        if min_size is None:
            min_size = int(self.client.conf_get(
                'osd_pool_default_size'))
        pass

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
            self._ec_pool_create()
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

    def pool_delete(self, pool_name):
        return self.client.delete_pool(pool_name)

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
        command_str = '{"prefix": "osd crush add", "args" : \
            ["host=%(host_name)s"], "id": %(osd_id)s, \
             "weight": %(weight)s}' \
                % {'osd_id': osd_id, 'host_name': host_name, 'weight': weight}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("add osd.{} to {} error: {}".format(
                osd_id, host_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

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

    def _send_mon_command(self, command_str):
        ret, outbuf, _ign = self.client.mon_command(command_str, '')
        if ret:
            raise CephException(
                message="execute command failed: {}".format(command_str))
        return get_json_output(outbuf)

    def _send_osd_command(self, osd_id, command_str):
        ret, outbuf, _ign = self.client.osd_command(osd_id, command_str, '')
        if ret:
            raise CephException(
                message="execute command failed: {}".format(command_str))
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

    def rule_add(self, rule_name='replicated_rule', root_name='default',
                 choose_type='host', device_class=None):
        cmd = {
            "root": root_name,
            "prefix": "osd crush rule create-replicated",
            "type": choose_type,
            "name": rule_name,
        }
        if device_class:
            cmd["class"] = device_class
        command_str = json.dumps(cmd)
        self._send_mon_command(command_str)

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
