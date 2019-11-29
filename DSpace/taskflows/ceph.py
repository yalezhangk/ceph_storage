#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import json
import logging
import math
import os

import six

from DSpace import exception as exc
from DSpace import objects
from DSpace.tools.ceph import RADOSClient

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
            self.key_file = '/etc/ceph/{}/ceph.client.admin.keyring'.format(
                ctxt.cluster_id)
            self._generate_config_file()
            self._generate_admin_keyring()

    def ceph_admin_keyring(self):
        admin_keyring = objects.CephConfig.get_by_key(
            self.ctxt, 'global', 'client.admin')
        return admin_keyring

    def ceph_config(self):
        content = objects.ceph_config.ceph_config_content(self.ctxt)
        return content

    def rados_args(self):
        obj = objects.CephConfig.get_by_key(self.ctxt, "global", "mon_host")
        res = {'mon_host': obj.value}
        admin_keyring = self.ceph_admin_keyring()
        if admin_keyring:
            res["keyring"] = self.key_file
        logger.info("ceph task, rados args: %s", res)
        return res

    def _generate_admin_keyring(self):
        admin_keyring = self.ceph_admin_keyring()
        config = configparser.ConfigParser()

        if not admin_keyring:
            return None
        if not os.path.exists(self.conf_dir):
            os.makedirs(self.conf_dir, mode=0o0755)
        config['client.admin'] = {}
        config['client.admin']['key'] = admin_keyring.value
        with open(self.key_file, 'w') as f:
            config.write(f)

    def _generate_config_file(self):
        ceph_config_str = self.ceph_config()
        logger.info("will generate ceph conf file: {}".format(
            ceph_config_str))
        if not os.path.exists(self.conf_dir):
            os.makedirs(self.conf_dir, mode=0o0755)
        with open(self.conf_file, 'w') as f:
            f.write(ceph_config_str)

    def get_ceph_df(self):
        with RADOSClient(self.rados_args(), timeout='2') as rados_client:
            return rados_client.get_ceph_df()

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
    def pool_create(self, pool_data):
        logger.debug("pool_data: %s", json.dumps(pool_data))
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
            rep_size = pool_data.get('rep_size')
            fault_domain = pool_data.get('fault_domain')
            pool_role = pool_data.get('pool_role')
            can_specified_rep = pool_data.get('specified_rep')
            pool_rep_size = rep_size if can_specified_rep else None

            if pool_name in rados_client.pool_list():
                logger.error("pool %s alread exists", pool_name)
                raise exc.PoolExists(pool=pool_name)

            rados_client.root_add(default_root_name)
            if fault_domain == "datacenter":
                if 'datacenter' in pool_data:
                    for d, r in six.iteritems(pool_data.get('datacenter')):
                        rados_client.datacenter_add(d)
                        for rack_name in r:
                            rados_client.rack_add(rack_name)
                            rados_client.rack_move_to_datacenter(rack_name, d)
                        rados_client.datacenter_move_to_root(
                            d, default_root_name)
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
            logger.info('creating pool pg_num: %s', pg_num)

            rados_client.rule_add(rule_name, default_root_name, fault_domain)
            rados_client.pool_create(pool_name=pool_name,
                                     pool_type=pool_type,
                                     rule_name=rule_name,
                                     pg_num=pg_num,
                                     pgp_num=pg_num,
                                     rep_size=pool_rep_size)
            rados_client.set_pool_application(pool_name, "rbd")
            if not can_specified_rep:
                rados_client.pool_set_replica_size(
                    pool_name=pool_name, rep_size=rep_size)
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
                                             rep_size=pool_rep_size)
                    rados_client.set_pool_application(pool, "rgw")
                    if not can_specified_rep:
                        rados_client.pool_set_replica_size(
                            pool_name=pool, rep_size=rep_size)

            return rados_client.get_pool_stats(pool_name).get('pool_id')

    def config_set(self, cluster_temp_configs):
        with RADOSClient(self.rados_args()) as rados_client:
            for config in cluster_temp_configs:
                service = config['service']
                osd_list = None
                if service.startswith('osd'):
                    osd_id = service.split('.')[1]
                    if osd_id == '*':
                        osd_list = objects.OsdList.get_all(self.ctxt)
                rados_client.config_set(service,
                                        config['key'],
                                        config['value'],
                                        osd_list)

    def rule_get(self, rule_name):
        with RADOSClient(self.rados_args()) as rados_client:
            rule_detail = rados_client.rule_get(rule_name)
            return rule_detail

    def _cal_pg_num(self, osd_num, rep_size=3):
        if not osd_num:
            return 0
        pg_num = (100 * osd_num) / rep_size
        # mon_max_pg_per_osd
        if (pg_num * 3 / osd_num) >= 250:
            pg_num = 250 * (osd_num / 3)
        pg_log2 = int(math.floor(math.log(pg_num, 2)))
        logger.debug("osd_num: {}, rep_size: {}, pg_log2: {}".format(
            osd_num, rep_size, pg_log2))
        if pg_num > (2**16):
            pg_log2 = 16
        return 2**pg_log2

    def pool_delete(self, data):
        logger.debug("pool_delete, pool_data: {}".format(data))
        pool_name = data.get('pool_name')
        root_name = data.get('root_name')
        rule_name = data.get('crush_rule_name')
        pool_role = data.get('pool_role')
        with RADOSClient(self.rados_args()) as rados_client:
            pool_list = rados_client.pool_list()
            if pool_name in pool_list:
                rados_client.pool_delete(pool_name)
            else:
                logger.debug("pool %s not exists, ignore it", pool_name)
            if pool_role == 'gateway':
                rgw_pools = ['.rgw.root', 'default.rgw.control',
                             'default.rgw.meta', 'default.rgw.log',
                             'default.rgw.buckets.index',
                             'default.rgw.buckets.non-ec',
                             'default.rgw.buckets.data']
                for pool in rgw_pools:
                    if pool in pool_list:
                        rados_client.pool_delete(pool)
                    else:
                        logger.debug("pool %s not exists, ignore it",
                                     pool)
            if rule_name != "replicated_rule":
                rados_client.rule_remove(rule_name)
            if 'host' in data:
                for h, o in six.iteritems(data.get('host')):
                    for osd_info in o:
                        osd_id, _ = osd_info[0], osd_info[1]
                        osd_name = "osd.{}".format(osd_id)
                        rados_client.bucket_remove(osd_name, ancestor=h)
                    rados_client.bucket_remove(h)
            if "rack" in data:
                for r, h in six.iteritems(data.get('rack')):
                    rados_client.bucket_remove(r)
            if "datacenter" in data:
                for d, r in six.iteritems(data.get('datacenter')):
                    rados_client.bucket_remove(d)
            rados_client.bucket_remove(root_name)

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
                        rados_client.datacenter_add(d)
                        for rack_name in r:
                            rados_client.rack_add(rack_name)
                            rados_client.rack_move_to_datacenter(rack_name, d)
                        rados_client.datacenter_move_to_root(
                            d, default_root_name)
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
        with RADOSClient(self.rados_args()) as rados_client:
            if 'host' in data:
                for h, o in six.iteritems(data.get('host')):
                    for osd_info in o:
                        osd_id, _ = osd_info[0], osd_info[1]
                        osd_name = "osd.{}".format(osd_id)
                        rados_client.bucket_remove(osd_name, ancestor=h)
                    if not rados_client.bucket_get(h):
                        rados_client.bucket_remove(h)
            if "rack" in data:
                for r, h in six.iteritems(data.get('rack')):
                    if not rados_client.bucket_get(r):
                        rados_client.bucket_remove(r)
            if "datacenter" in data:
                for d, r in six.iteritems(data.get('datacenter')):
                    if not rados_client.bucket_get(d):
                        rados_client.bucket_remove(d)

    def cluster_info(self):
        with RADOSClient(self.rados_args(), timeout='1') as rados_client:
            return rados_client.get_cluster_info()

    def update_pool_policy(self, data):
        rep_size = data.get('rep_size')
        pool_name = data.get('pool_name')
        root_name = data.get('root_name')
        fault_domain = data.get('fault_domain')
        rule_name = data.get('crush_rule_name')
        if pool_name is None:
            raise exc.CephException(
                message='pool name must be specifed while update policy')
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            rados_client.pool_set_replica_size(pool_name=pool_name,
                                               rep_size=rep_size)
            if fault_domain == "datacenter":
                if 'datacenter' in data:
                    for d, r in six.iteritems(data.get('datacenter')):
                        rados_client.datacenter_add(d)
                        rados_client.datacenter_move_to_root(
                            d, root_name)
                        for rack_name in r:
                            rados_client.rack_add(rack_name)
                            rados_client.rack_move_to_datacenter(rack_name, d)
                if 'rack' in data:
                    for r, h in six.iteritems(data.get('rack')):
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
            if fault_domain == "rack":
                if 'rack' in data:
                    for r, h in six.iteritems(data.get('rack')):
                        rados_client.rack_add(r)
                        rados_client.rack_move_to_root(r, root_name)
                        for host_name in h:
                            rados_client.host_add(host_name)
                            rados_client.host_move_to_rack(host_name, r)
            tmp_rule_name = "{}-new".format(rule_name)
            rados_client.rule_add(tmp_rule_name, root_name, fault_domain)
            rados_client.set_pool_info(pool_name, "crush_rule", tmp_rule_name)
            rados_client.rule_remove(rule_name)
            rados_client.rule_rename(tmp_rule_name, rule_name)
            return rados_client.rule_get(rule_name)

    def osd_new(self, osd_fsid):
        with RADOSClient(self.rados_args()) as rados_client:
            return rados_client.osd_new(osd_fsid)

    def get_pools(self):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.get_pools()

    def get_pool_info(self, pool_name="rbd", keyword="size"):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.get_pool_info(pool_name, keyword)

    def get_crush_rule_info(self, rule_name="replicated_rule"):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.get_crush_rule_info(rule_name)

    def get_bucket_info(self, bucket):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.bucket_get(bucket)
