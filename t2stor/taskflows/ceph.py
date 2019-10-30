#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import math
import os

import six

from t2stor import exception as exc
from t2stor import objects
from t2stor.tools.ceph import RADOSClient
from t2stor.tools.ceph import RBDProxy

logger = logging.getLogger(__name__)


class CephTask(object):
    ctxt = None

    def __init__(self, ctxt):
        self.ctxt = ctxt
        if ctxt:
            self.conf_dir = '/etc/ceph/{}/'.format(
                ctxt.cluster_id)
            self.conf_file = '/etc/ceph/{}/ceph.conf'.format(
                ctxt.cluster_id)
            self._generate_config_file()

    def ceph_config(self):
        content = objects.ceph_config.ceph_config_content(self.ctxt)
        return content

    def rados_args(self):
        obj = objects.CephConfig.get_by_key("default", "mon_host")
        return {'mon_host': obj.value}

    def _generate_config_file(self):
        ceph_config_str = self.ceph_config()
        logger.info("will generate ceph conf file: {}".format(
            ceph_config_str))
        if not os.path.exists(self.conf_dir):
            os.mkdir(self.conf_dir, mode=0o0755)
        with open(self.conf_file, 'w') as f:
            f.write(ceph_config_str)

    def pool_add_osd(self):
        pass

    def pool_rm_osd(self):
        pass

    def pool_rm(self):
        pass

    """
    Get all osds in a ceph cluster
    """
    def get_osds(self):
        pass

    """
    Get osd capacity
    """
    def get_osd_df(self):
        with RADOSClient(self.rados_args(), timeout='2') as rados_client:
            return rados_client.get_osd_df()

    """
    Get all osds in a pool
    """
    def get_pool_osds(self, pool_name):
        with RADOSClient(self.rados_args()) as rados_client:
            osds = rados_client.get_osds_by_pool(pool_name)
            return osds

    """
    Get all osds in a host
    """
    def get_host_osds(self, host_name):
        pass

    """
    pool_type: replicated/erasure 资源池的类型（副本类型或者纠删码类型）
    rep_size: 使用副本类型时的副本数
    fault_domain: 使用副本类型时的故障级别（host、rack或者datacenter），默认为host

    Example:
    pool_data
    {
        "pool_name":"test",
        "pool_type":"replicated/erasure",
        "ec_profile":"plugin=jerasure technique=reed_sol_van k=2 m=1",
        "rep_size":3,
        "fault_domain":"host/rack/datacenter",
        "root_name": "root-test",
        "datacenter":{
            "d1":[
                "r1",
            ],
            "d2":[
                "r2",
            ]
        },
        "rack":{
            "r1":[
                "h1",
            ],
            "r2":[
                "h2",
            ]
        },
        "host":{
            "h1":[
                0,
            ],
            "h2":[
                3,
            ]
        }
    }
    """

    # TODO
    # 1.检查传入参数
    # 2.自动计算pg数
    def pool_create(self, pool_data):
        logger.debug("pool_data: {}".format(json.dumps(pool_data)))
        osds = []
        with RADOSClient(self.rados_args()) as rados_client:
            # 1. Create bucket: host, rack, [datacenter], root
            # 2. Move osd to host, move host to rack...
            # 3. Get current crushmap
            # 4. Create a new rule set according to choose_type
            # 5. Set the new crushmap
            default_root_name = pool_data.get('root_name')
            pool_name = pool_data.get('pool_name')
            rule_name = pool_data.get('crush_rule_name')
            pool_type = pool_data.get('pool_type')
            # ec_profile = pool_data.get('ec_profile')
            rep_size = pool_data.get('rep_size')
            fault_domain = pool_data.get('fault_domain')
            pool_role = pool_data.get('pool_role')

            if pool_name in rados_client.pool_list():
                raise exc.PoolExists(pool=pool_name)

            rados_client.root_add(default_root_name)
            if fault_domain == "datacenter":
                if 'datacenter' in pool_data:
                    for d, r in six.iteritems(pool_data.get('datacenter')):
                        rados_client.datacenter_add(d)
                        for rack_name in r:
                            rados_client.rack_add(rack_name)
                            rados_client.rack_move_to_datacenter(rack_name, d)
                        rados_client.datacenter_move(d, default_root_name)
                if 'rack' in pool_data:
                    for r, h in six.iteritems(pool_data.get('rack')):
                        rados_client.rack_add(r)
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
                if 'host' in pool_data:
                    for h, o in six.iteritems(pool_data.get('host')):
                        rados_client.host_add(h)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)

            if fault_domain == "rack":
                if 'rack' in pool_data:
                    for r, h in six.iteritems(pool_data.get('rack')):
                        rados_client.rack_add(r)
                        rados_client.rack_move_to_root(r, default_root_name)
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
                if 'host' in pool_data:
                    for h, o in six.iteritems(pool_data.get('host')):
                        rados_client.host_add(h)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)

            if fault_domain == 'host':
                if 'host' in pool_data:
                    for h, o in six.iteritems(pool_data.get('host')):
                        rados_client.host_add(h)
                        rados_client.host_move_to_root(h, default_root_name)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)
            pg_num = self._cal_pg_num(len(osds), rep_size)
            logger.debug('pg_num: {}'.format(pg_num))
            crush_file = '/tmp/.crushmap'
            crush_with_rule_file = '/tmp/.crushmap.add_rule'
            if rados_client.get_crushmap(crush_file):
                rados_client.rule_add(crush_file, crush_with_rule_file,
                                      rule_name=rule_name,
                                      root_name=default_root_name,
                                      choose_type=fault_domain)
                rados_client.set_crushmap(crush_with_rule_file)
                rados_client.pool_create(pool_name=pool_name,
                                         pool_type=pool_type,
                                         rule_name=rule_name,
                                         pg_num=pg_num,
                                         pgp_num=pg_num,
                                         rep_size=rep_size)
                if pool_role == 'gateway':
                    pg_num = 32
                    rgw_pools = ['.rgw.root', 'default.rgw.control',
                                 'default.rgw.meta', 'default.rgw.log',
                                 'default.rgw.buckets.index',
                                 'default.rgw.buckets.non-ec',
                                 'default.rgw.buckets.data']
                    for pool in rgw_pools:
                        rados_client.pool_create(pool_name=pool,
                                                 pool_type=pool_type,
                                                 rule_name=rule_name,
                                                 pg_num=pg_num,
                                                 pgp_num=pg_num,
                                                 rep_size=rep_size)

            os.remove(crush_with_rule_file)
            return rados_client.get_pool_stats(pool_name).get('pool_id')

    def config_set(self, cluster_temp_configs):
        with RADOSClient(self.rados_args()) as rados_client:
            for config in cluster_temp_configs:
                rados_client.config_set(config['service'],
                                        config['key'],
                                        config['value'])

    def _cal_pg_num(self, osd_num, rep_size):
        if not osd_num:
            return 0
        pg_num = (100 * osd_num) / rep_size
        pg_log2 = int(math.floor(math.log(pg_num, 2)))
        logger.debug("osd_num: {}, rep_size: {}, pg_log2: {}".format(
            osd_num, rep_size, pg_log2))
        if pg_num > (2**16):
            pg_log2 = 16
        return 2**pg_log2

    """
    {
        "pool_name":"test",
        "crush_rule_name":"rule-test",
        "root_name": "root-test",
        "datacenters": ["da1", "da2"],
        "hosts":["nod1","nod2"],
        "racks":["rack1","rack2"],
        "osds":[
            "osd.0",
            "osd.1"
        ]
    }
    """
    def pool_delete(self, data):
        logger.debug("pool_data: {}".format(data))
        pool_name = data.get('pool_name')
        root = data.get('root_name')
        rule_name = data.get('crush_rule_name')
        pool_role = data.get('pool_role')
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            rados_client.pool_delete(pool_name)
            if pool_role == 'gateway':
                rgw_pools = ['.rgw.root', 'default.rgw.control',
                             'default.rgw.meta', 'default.rgw.log',
                             'default.rgw.buckets.index',
                             'default.rgw.buckets.non-ec',
                             'default.rgw.buckets.data']
                for pool in rgw_pools:
                    rados_client.pool_delete(pool)
            # with CrushmapTool() as crushmap_tool:
            rados_client.rule_remove(rule_name)
            if 'osds' in data:
                osds = data.get('osds')
                for osd in osds:
                    rados_client.bucket_remove(osd)
            if 'nodes' in data:
                hosts = data.get('nodes')
                for host in hosts:
                    rados_client.bucket_remove(host)
            if 'racks' in data:
                racks = data.get('racks')
                for rack in racks:
                    rados_client.bucket_remove(rack)
            if 'datacenters' in data:
                datacenters = data.get('datacenters')
                for dc in datacenters:
                    rados_client.bucket_remove(dc)
            rados_client.bucket_remove(root)

    """
    Example:
    pool_data
    {
        "pool_name":"test",
        "root_name":"root-test",
        "crush_rule_name":"rule-test",
        "fault_domain":"host/rack/datacenter",
        "datacenter":{
            "d1":[
                "r1",
            ],
            "d2":[
                "r2",
            ]
        },
        "rack":{
            "r1":[
                "h1",
            ],
            "r2":[
                "h2",
            ]
        },
        "host":{
            "h1":[
                0,
            ],
            "h2":[
                3,
            ]
        }
    }
    """
    # 1. Add osd to host
    # 2. Move osd to host, move host to rack...
    # XXX crush_rule_name will not be used
    def pool_add_disk(self, data):
        logger.debug("data: {}".format(json.dumps(data)))
        osds = []
        pool_name = data.get('pool_name')
        root_name = data.get('root_name')
        with RADOSClient(self.rados_args()) as rados_client:
            default_root_name = data.get('root_name')
            fault_domain = data.get('fault_domain')
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)

            if fault_domain == "datacenter":
                if 'datacenter' in data:
                    for d, r in six.iteritems(data.get('datacenter')):
                        # FIXME datacenter may be exists,
                        #  but add once again is ok
                        rados_client.datacenter_add(d)
                        for rack_name in r:
                            rados_client.rack_add(rack_name)
                            rados_client.rack_move_to_datacenter(rack_name, d)
                        rados_client.datacenter_move(d, default_root_name)
                if 'rack' in data:
                    for r, h in six.iteritems(data.get('rack')):
                        rados_client.rack_add(r)
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
                if 'host' in data:
                    for h, o in six.iteritems(data.get('host')):
                        rados_client.host_add(h)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)

            if fault_domain == "rack":
                if 'rack' in data:
                    for r, h in six.iteritems(data.get('rack')):
                        rados_client.rack_add(r)
                        rados_client.rack_move_to_root(r, default_root_name)
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
                if 'host' in data:
                    for h, o in six.iteritems(data.get('host')):
                        rados_client.host_add(h)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)

            if fault_domain == 'host':
                if 'host' in data:
                    for h, o in six.iteritems(data.get('host')):
                        rados_client.host_add(h)
                        rados_client.host_move_to_root(h, default_root_name)
                        for osd_info in o:
                            osds.append(o)
                            osd_id, osd_size = osd_info[0], osd_info[1]
                            rados_client.osd_add(osd_id, osd_size, h)
            pool_rep_size = rados_client.get_pool_info(
                pool_name, 'size').get('size')
            pool_pg_num = rados_client.get_pool_info(
                pool_name, 'pg_num').get('pg_num')
            total_osds = rados_client.get_osds_by_bucket(root_name)
            new_pg_num = self._cal_pg_num(len(total_osds), pool_rep_size)
            max_split_count = int(rados_client.conf_get(
                'mon_osd_max_split_count'))
            if new_pg_num > pool_pg_num and\
                    (new_pg_num / len(total_osds) <= max_split_count):
                logger.debug("will increate pg to {}".format(new_pg_num))
                rados_client.set_pool_info(pool_name, 'pg_num', new_pg_num)
                rados_client.set_pool_info(pool_name, 'pgp_num', new_pg_num)

    def pool_del_disk(self, data):
        logger.debug("pool_data: {}".format(data))
        # pool_name = data.get('pool_name')
        root = data.get('root_name')
        # rule_name = data.get('crush_rule_name')
        with RADOSClient(self.rados_args()) as rados_client:
            # with CrushmapTool() as crushmap_tool:
            if 'osds' in data:
                osds = data.get('osds')
                for osd in osds:
                    rados_client.bucket_remove(osd)
            if 'nodes' in data:
                hosts = data.get('nodes')
                for host in hosts:
                    rados_client.bucket_remove(host)
            if 'racks' in data:
                racks = data.get('racks')
                for rack in racks:
                    rados_client.bucket_remove(rack)
            if 'datacenters' in data:
                datacenters = data.get('datacenters')
                for dc in datacenters:
                    rados_client.bucket_remove(dc)
            rados_client.bucket_remove(root)

    def cluster_info(self):
        with RADOSClient(self.rados_args(), timeout='1') as rados_client:
            return rados_client.get_cluster_info()

    # TODO will update crushmap
    def update_pool_policy(self, pool_data):
        rep_size = pool_data.get('rep_size')
        pool_name = pool_data.get('pool_name')
        if pool_name is None:
            raise exc.CephException(
                message='pool name must be specifed while update policy')
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            rados_client.pool_set_replica_size(pool_name=pool_name,
                                               rep_size=rep_size)

    def rbd_list(self, pool_name):
        with RADOSClient(self.rados_args(), timeout='1') as rados_client:
            if pool_name not in rados_client.pool_list():
                logger.warning("{} not found".format(pool_name))
                return []
            with RBDProxy(rados_client, pool_name) as rbd_client:
                return rbd_client.rbd_list()

    def rbd_size(self, pool_name, rbd_name):
        with RADOSClient(self.rados_args(), timeout='1') as rados_client:
            with RBDProxy(rados_client, pool_name) as rbd_client:
                return rbd_client.rbd_size(rbd_name)

    def rbd_snap_create(self, pool_name, rbd_name, snap_name):
        with RADOSClient(self.rados_args()) as rados_client:
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_create(rbd_name, snap_name)

    def rbd_create(self, pool_name, rbd_name, rbd_size):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_create(rbd_name, rbd_size)

    def rbd_remove(self, pool_name, rbd_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_remove(rbd_name)

    def rbd_rename(self, pool_name, old_rbd_name, new_rbd_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_rename(old_rbd_name, new_rbd_name)

    def rbd_snap_remove(self, pool_name, rbd_name, snap_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_remove(rbd_name, snap_name)

    def rbd_snap_rename(self, pool_name, rbd_name, old_name, new_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_rename(rbd_name, old_name, new_name)

    def rbd_clone_volume(self, p_p_name, p_v_name, p_s_name, c_p_anme,
                         c_v_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if p_p_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=p_p_name)
            if c_p_anme not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=p_p_name)
            with RBDProxy(rados_client, p_p_name) as rbd_client:
                with RBDProxy(rados_client, c_p_anme) as c_rbd_client:
                    rbd_client.rbd_clone(p_v_name, p_s_name,
                                         c_rbd_client.io_ctx, c_v_name)

    def rbd_is_protect_snap(self, pool_name, volume_name, snap_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                return rbd_client.is_protect_snap(volume_name, snap_name)

    def rbd_protect_snap(self, pool_name, volume_name, snap_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.protect_snap(volume_name, snap_name)

    def rbd_unprotect_snap(self, pool_name, volume_name, snap_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_unprotect_snap(volume_name, snap_name)

    def rbd_flatten(self, c_p_name, c_v_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if c_p_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=c_p_name)
            with RBDProxy(rados_client, c_p_name) as rbd_client:
                rbd_client.rbd_image_flatten(c_v_name)

    def rbd_rollback_to_snap(self, pool_name, v_name, s_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_rollback_to_snap(v_name, s_name)

    def rbd_resize(self, pool_name, v_name, size):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_resize(v_name, size)
