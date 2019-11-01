#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

from oslo_utils import encodeutils

from t2stor.exception import CephException
from t2stor.exception import RunCommandError
from t2stor.tools.base import ToolBase

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

    def disk_prepare(self, backend, diskname, db_partition=None,
                     wal_partition=None, cache_partition=None,
                     journal_partition=None, fsid=None, osd_id=None):
        # ceph-disk --setuser root --setgroup root prepare
        # --osd-uuid {{ osd_fsid }} --osd-id {{ osd_id}}
        # --cluster {{ cluster }} --{{ store_backend }}
        # --block.db {{ item.0 }} --block.t2ce {{ item.1 }} {{ item.2 }}"
        cmd = ["ceph-disk", "--setuser", "ceph", "--setgroup", "ceph",
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
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def disk_active(self, diskname):
        cmd = ["ceph-disk", "activate", "/dev/%s1" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_deactivate(self, diskname):
        cmd = ["ceph-disk", "deactivate", "/dev/%s1" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)

    def osd_zap(self, diskname):
        cmd = ["ceph-disk", "zap", "/dev/%s" % diskname]
        rc, stdout, stderr = self.run_command(cmd, timeout=300)
        if rc:
            raise RunCommandError(cmd=cmd, return_code=rc,
                                  stdout=stdout, stderr=stderr)
        return True


def get_json_output(json_databuf):
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

    """
    XXX crush_rule is a integer
    """
    # def pool_create(self, pool_name=DEFAULT_POOL, crush_rule):
    #     try:
    #         self.client.create_pool(pool_name)
    #     except rados.Error:
    #         raise

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
                    ec_profile=None, pg_num=None, pgp_num=None, rep_size=0):
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
                                pg_num=None, pgp_num=None, rep_size=0):
        """
        osd pool create <poolname> <int[0-]> {<int[0-]>} {replicated|erasure} \
            {<erasure_code_profile>} {<rule>} {<int>} :  create pool

        FIXME Only support create replicated pool now
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

    def config_set(self, service, key, value):
        cmd = {
            "prefix": "config set",
            "key": key,
            "value": value
        }
        command_str = json.dumps(cmd)
        if value.get('service').startswith('osd'):
            self._send_osd_command(service, command_str)
        if value.get('service').startswith('mon'):
            self._send_mon_command(command_str)

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

    def datacenter_move(self, datacenter_name, root_name):
        """
        Move a datacenter to a specified root
        {
            "prefix": "osd crush move",
            "args": ["root=root-test"],
            "name": "datacenter-1"
        }
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["root=%(root_name)s"], "name": "%(datacenter_name)s"}'\
            % {'root_name': root_name, 'datacenter_name': datacenter_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                datacenter_name, root_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def rack_move_to_root(self, rack_name, root_name):
        """
        Move a rack to a root
        {
            "prefix": "osd crush move",
            "args": ["root=root-test"],
            "name": "rack-test"
        }
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["root=%(root_name)s"], "name": "%(rack_name)s"}'\
                % {'root_name': root_name, 'rack_name': rack_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                rack_name, root_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def rack_move_to_datacenter(self, rack_name, datacenter_name):
        """
        Move a rack to a datacenter
        {
            "prefix": "osd crush move",
            "args": ["datacenter=datacenter-1"],
            "name": "rack-test"
        }
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["datacenter=%(datacenter)s"], "name": "%(rack_name)s"}'\
                % {'datacenter': datacenter_name, 'rack_name': rack_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                rack_name, datacenter_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def host_move_to_root(self, host_name, root_name):
        """
        Move a host to a specified rack
        ceph osd crush move ceph-3 root=root-default
        {
            "prefix": "osd crush move",
            "args": ["root=root-default"],
            "name": "ceph-3"
        }
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["root=%(root_name)s"], "name": "%(host_name)s"}'\
                % {'root_name': root_name, 'host_name': host_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                host_name, root_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def bucket_remove(self, bucket):
        """
        {"prefix": "osd crush remove", "name": "da2"}
        """
        command_str = '{"prefix": "osd crush remove", "name": "%(bucket)s"}' \
                      % {'bucket': bucket}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("remove bucket {} error: {}".format(
                bucket, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def host_move_to_rack(self, host_name, rack_name):
        """
        Move a host to a specified rack
        ceph osd crush move ceph-3 rack=rack-xxx
        [{"prefix": "osd crush move", "args": ["rack=rack-xxx"],
          "name": "ceph-3"}]
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["rack=%(rack_name)s"], "name": "%(host_name)s"}'\
                % {'rack_name': rack_name, 'host_name': host_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                host_name, rack_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def osd_move_to_root(self, osd_name, root_name='default'):
        """
        Move a osd to a default root
        ceph osd crush move osd.1 root=default
        '[{"prefix": "osd crush move", "args" : ["root=default"], \
            "name": "osd.4"}]'
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["root=%(root_name)s"], "name": "%(osd_name)s"}'\
                % {'osd_name': osd_name, 'root_name': root_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                osd_name, root_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def osd_move(self, osd_name, host_name):
        """
        Move a osd to a specified host
        ceph osd crush move osd.1 host=ceph-3
        '[{"prefix": "osd crush move", "args" : ["host=ceph-3.1"], \
            "name": "osd.4"}]'
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["host=%(host_name)s"], "name": "%(osd_name)s"}'\
                % {'osd_name': osd_name, 'host_name': host_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("{} move to {} error: {}".format(
                osd_name, host_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

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

    def root_add(self, root_name):
        """
        ceph osd crush add-bucket root-xxx root
        [{"prefix": "osd crush add-bucket", "type": "root",
          "name": "root-xxx"}]
        """
        command_str = '{"prefix": "osd crush add-bucket", "type": "root", \
            "name": "%(root_name)s"}' % {'root_name': root_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("root add {} error: {}".format(
                root_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def datacenter_add(self, datacenter_name):
        """
        {
            "prefix": "osd crush add-bucket",
            "type": "datacenter",
            "name": "datacenter-1"
        }
        """
        command_str = '{"prefix": "osd crush add-bucket", \
            "type": "datacenter", "name": "%(datacenter_name)s"}' \
            % {'datacenter_name': datacenter_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("datacenter add {} error: {}".format(
                datacenter_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def rack_add(self, rack_name):
        """
        Add a rack bucket type
        ceph osd crush add-bucket rack-xxx rack
        [{"prefix": "osd crush add-bucket", "type": "rack",
          "name": "rack-xxx"}]
        """
        command_str = '{"prefix": "osd crush add-bucket", "type": "rack", \
            "name": "%(rack_name)s"}' % {'rack_name': rack_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("rack add {} error: {}".format(
                rack_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def host_add(self, host_name):
        """
        Add a host bucket type
        ceph osd crush add host-xxx host
        [{"prefix": "osd crush add-bucket", "type": "host",
          "name": "host-xxxx"}]
        """
        command_str = '{"prefix": "osd crush add-bucket", "type": "host", \
            "name": "%(host_name)s"}' % {'host_name': host_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("host add {} error: {}".format(
                host_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

    def osd_rm(self, osd_name, root_name='default'):
        """
        XXX
        Just move it to default, so it will not in any host
        """
        command_str = '{"prefix": "osd crush move", "args" : \
            ["root=%(root_name)s"], "name": "%(osd_name)s"}'\
                % {'osd_name': osd_name, 'root_name': root_name}
        logger.debug('command_str: {}'.format(command_str))
        ret, mon_dump_outbuf, __ = self.client.mon_command(command_str, '')
        if ret:
            logger.error("remove osd {} error: {}".format(
                osd_name, mon_dump_outbuf))
            raise CephException(
                message="execute command failed: {}".format(command_str))
        logger.debug(mon_dump_outbuf)

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
