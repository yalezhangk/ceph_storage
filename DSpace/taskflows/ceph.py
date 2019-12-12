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
from DSpace.objects.fields import FaultDomain
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

    def _crush_rule_create(self, rados_client, crush_content):
        logger.info("rule create: %s", json.dumps(crush_content))
        rule_name = crush_content.get('crush_rule_name')
        default_root_name = crush_content.get('root_name')
        rados_client.root_add(default_root_name)
        fault_domain = crush_content.get('fault_domain')
        datacenters = crush_content.get('datacenters')
        racks = crush_content.get('racks')
        hosts = crush_content.get('hosts')
        osds = crush_content.get('osds')
        # add datacenter
        if fault_domain == FaultDomain.DATACENTER:
            for name, dc in six.iteritems(datacenters):
                rados_client.datacenter_add(dc['crush_name'])
                rados_client.datacenter_move_to_root(
                    dc['crush_name'], default_root_name)
                dc_racks = [racks.get(rack_id)
                            for rack_id in dc['racks']]
                for rack in dc_racks:
                    rados_client.rack_add(rack['crush_name'])
                    rados_client.rack_move_to_datacenter(
                        rack['crush_name'], dc['crush_name'])
        # move rack to root
        if fault_domain == FaultDomain.RACK:
            for name, rack in six.iteritems(crush_content.get('racks')):
                rados_client.rack_add(rack['crush_name'])
                rados_client.rack_move_to_root(
                    rack['crush_name'], default_root_name)
        # add host
        if fault_domain in [FaultDomain.DATACENTER, FaultDomain.RACK]:
            for name, rack in six.iteritems(racks):
                rack_hosts = [hosts.get(name)
                              for name in rack['hosts']]
                for host in rack_hosts:
                    rados_client.host_add(host['crush_name'])
                    rados_client.host_move_to_rack(
                        host['crush_name'], rack['crush_name'])
        # move host to root
        if fault_domain == FaultDomain.HOST:
            for name, host in six.iteritems(crush_content.get('hosts')):
                rados_client.host_add(host['crush_name'])
                rados_client.rack_move_to_root(
                    host['crush_name'], default_root_name)
        # add osd
        for name, host in six.iteritems(hosts):
            host_osds = [osds.get(osd_id) for osd_id in host['osds']]
            for osd in host_osds:
                rados_client.osd_add(osd['id'], osd['size'],
                                     host['crush_name'])

        rados_client.rule_add(rule_name, default_root_name, fault_domain)

    def pool_create(self, pool, can_specified_rep, crush_content):
        logger.debug("pool_data: %s", json.dumps(crush_content))
        with RADOSClient(self.rados_args()) as rados_client:
            # 1. Create bucket: host, rack, [datacenter], root
            # 2. Move osd to host, move host to rack...
            # 3. Get current crushmap
            # 4. Create a new rule set according to choose_type
            # 5. Set the new crushmap
            pool_name = pool.pool_name
            pool_type = pool.type
            rep_size = pool.replicate_size
            pool_role = pool.role
            rule_name = crush_content.get('crush_rule_name')
            osd_num = len(crush_content.get('osds'))
            pool_rep_size = rep_size if can_specified_rep else None

            if pool_name in rados_client.pool_list():
                logger.error("pool %s alread exists", pool_name)
                raise exc.PoolExists(pool=pool_name)

            pg_num = self._cal_pg_num(osd_num, rep_size)
            logger.info('creating pool pg_num: %s', pg_num)
            self._crush_rule_create(rados_client, crush_content)

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
                rgw_pools = ['.rgw.root',
                             'default.rgw.meta',
                             'default.rgw.buckets.index',
                             'default.rgw.buckets.non-ec']
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

    def _crush_rule_delete(self, client, crush_content):
        logger.info("rule delete: %s", json.dumps(crush_content))
        rule_name = crush_content.get('crush_rule_name')
        root_name = crush_content.get('root_name')
        fault_domain = crush_content.get('fault_domain')
        datacenters = crush_content.get('datacenters')
        racks = crush_content.get('racks')
        hosts = crush_content.get('hosts')
        osds = crush_content.get('osds')
        # delete rule
        client.rule_remove(rule_name)
        # remove osd and host
        for name, host in six.iteritems(hosts):
            host_osds = [osds.get(osd_name) for osd_name in host['osds']]
            for osd in host_osds:
                client.bucket_remove(osd['name'], ancestor=host['crush_name'])
            client.bucket_remove(host['crush_name'])
        logger.info("rule remove osd and host success")
        # remove rack
        if fault_domain in [FaultDomain.DATACENTER, FaultDomain.RACK]:
            for name, rack in six.iteritems(racks):
                client.bucket_remove(rack['crush_name'])
        logger.info("rule remove rack success")
        # remove datacenter
        if fault_domain == FaultDomain.DATACENTER:
            for name, dc in six.iteritems(datacenters):
                client.bucket_remove(dc['crush_name'])
        logger.info("rule remove datacenter success")
        client.bucket_remove(root_name)
        logger.info("rule remove rule success")

    def pool_delete(self, pool):
        logger.debug("pool_delete, pool_data: %s", pool)
        pool_name = pool.pool_name
        pool_role = pool.role
        with RADOSClient(self.rados_args()) as rados_client:
            rados_client.pool_delete(pool_name)
            if pool_role == 'gateway':
                rgw_pools = ['.rgw.root',
                             'default.rgw.meta',
                             'default.rgw.buckets.index',
                             'default.rgw.buckets.non-ec']
                for pool in rgw_pools:
                    rados_client.pool_delete(pool)

    def crush_delete(self, crush_content):
        logger.debug("crush_delete, data: %s", crush_content)
        with RADOSClient(self.rados_args()) as rados_client:
            self._crush_rule_delete(rados_client, crush_content)

    def _crush_rule_update(self, client, crush_content):
        logger.info("crush rule useless delete: %s", json.dumps(crush_content))
        fault_domain = crush_content.get('fault_domain')
        root_name = crush_content.get('root_name')
        datacenters = crush_content.get('datacenters')
        racks = crush_content.get('racks')
        hosts = crush_content.get('hosts')
        osds = crush_content.get('osds')

        def _add_dc_to_root():
            # create datacenter and move to root
            logger.info("start add datacenter to root")
            bucket_dcs = client.bucket_get(root_name)
            logger.debug("get datacenters from crush: %s", bucket_dcs)
            for name, dc in six.iteritems(datacenters):
                if dc['crush_name'] not in bucket_dcs:
                    client.datacenter_add(dc['crush_name'])
                    client.datacenter_move_to_root(
                        dc['crush_name'], root_name)
            # move rack to dc
            _move_rack_to_dc()

        def _move_rack_to_dc():
            logger.info("start move rack")
            for name, dc in six.iteritems(datacenters):
                dc_racks = [racks.get(rack_id)
                            for rack_id in dc['racks']]
                # get racks from ceph
                bucket_racks = client.bucket_get(dc['crush_name'])
                logger.debug("get rack from crush(%s): %s",
                             dc['crush_name'], bucket_racks)
                for rack in dc_racks:
                    if rack['crush_name'] in bucket_racks:
                        continue
                    client.rack_add(rack['crush_name'])
                    client.rack_move_to_datacenter(
                        rack['crush_name'], dc['crush_name'])

        def _add_rack_to_root():
            logger.info("start add rack to root")
            bucket_racks = client.bucket_get(root_name)
            logger.debug("get rack from crush(%s): %s",
                         root_name, bucket_racks)
            for name, rack in six.iteritems(crush_content.get('racks')):
                if rack['crush_name'] not in bucket_racks:
                    client.rack_add(rack['crush_name'])
                    client.rack_move_to_root(
                        rack['crush_name'], root_name)
            _move_host_to_rack()

        def _move_host_to_rack():
            logger.info("start update host crush resource")
            for name, rack in six.iteritems(racks):
                rack_hosts = [hosts.get(name)
                              for name in rack['hosts']]
                # get host from ceph
                bucket_hosts = client.bucket_get(rack['crush_name'])
                logger.debug("get host from crush(%s): %s",
                             rack['crush_name'], bucket_hosts)
                for host in rack_hosts:
                    if host['crush_name'] in bucket_hosts:
                        continue
                    client.host_add(host['crush_name'])
                    client.host_move_to_rack(
                        host['crush_name'], rack['crush_name'])

        def _add_host_to_root():
            bucket_hosts = client.bucket_get(root_name)
            logger.debug("get host from crush(%s): %s",
                         root_name, bucket_hosts)
            for name, host in six.iteritems(crush_content.get('hosts')):
                if host['crush_name'] not in bucket_hosts:
                    client.host_add(host['crush_name'])
                    client.rack_move_to_root(
                        host['crush_name'], root_name)
            _move_osd_to_host()

        def _move_osd_to_host():
            # add osd
            logger.info("start update osd crush resource")
            for name, host in six.iteritems(hosts):
                host_osds = [osds.get(osd_name) for osd_name in host['osds']]
                # get host from ceph
                bucket_osds = client.bucket_get(host['crush_name'])
                logger.debug("get osd from crush(%s): %s, adjust to osd: %s",
                             host['crush_name'], bucket_osds, host_osds)
                for osd in host_osds:
                    if osd['name'] in bucket_osds:
                        continue
                    client.osd_add(osd['id'], osd['size'],
                                   host['crush_name'])

        if fault_domain == FaultDomain.DATACENTER:
            _add_dc_to_root()
            _move_host_to_rack()
            _move_osd_to_host()

        # move rack to root
        if fault_domain == FaultDomain.RACK:
            _add_rack_to_root()
            _move_osd_to_host()

        # move host to root
        if fault_domain == FaultDomain.HOST:
            _add_host_to_root()

        self._crush_useless_delete(client, crush_content)

    def _crush_useless_delete(self, client, crush_content):
        logger.info("crush rule update: %s", json.dumps(crush_content))
        fault_domain = crush_content.get('fault_domain')
        root_name = crush_content.get('root_name')
        datacenters = crush_content.get('datacenters')
        racks = crush_content.get('racks')
        hosts = crush_content.get('hosts')
        osds = crush_content.get('osds')
        crush_dcs = []
        crush_racks = []
        crush_hosts = []
        useless_dcs = []
        useless_racks = []
        useless_hosts = []
        useless_osds = []

        def _check_dc_by_root():
            # check useless datacenters
            bucket_dcs = client.bucket_get(root_name)
            crush_dcs.extend(bucket_dcs)
            for name, dc in six.iteritems(datacenters):
                if dc['crush_name'] in bucket_dcs:
                    bucket_dcs.remove(dc['crush_name'])
            useless_dcs.extend(bucket_dcs)

        def _check_rack_by_dc():
            logger.info("start check rack")
            bucket_racks = []
            for name in crush_dcs:
                # get racks from ceph
                _bucket_racks = client.bucket_get(name)
                logger.debug("get rack from crush(%s): %s",
                             name, bucket_racks)
                bucket_racks.extend(_bucket_racks)
            crush_racks.extend(bucket_racks)
            for name, rack in six.iteritems(racks):
                if rack['crush_name'] in bucket_racks:
                    bucket_racks.remove(rack['crush_name'])
            # add useless rack to bucket_racks
            if bucket_racks:
                useless_racks.extend(bucket_racks)

        def _check_rack_by_root():
            # check useless racks
            bucket_racks = client.bucket_get(root_name)
            crush_racks.extend(bucket_racks)
            for name, rack in six.iteritems(racks):
                if rack['crush_name'] in bucket_racks:
                    bucket_racks.remove(rack['crush_name'])
            useless_racks.extend(bucket_racks)

        def _check_host_by_rack():
            logger.info("start check host crush resource")
            bucket_hosts = []
            for name in crush_racks:
                # get host from ceph
                _bucket_hosts = client.bucket_get(name)
                logger.debug("get host from crush(%s): %s",
                             name, _bucket_hosts)
                bucket_hosts.extend(_bucket_hosts)
            crush_hosts.extend(bucket_hosts)
            for name, host in six.iteritems(hosts):
                if host['crush_name'] in bucket_hosts:
                    bucket_hosts.remove(host['crush_name'])
            # add useless host to bucket_hosts
            if bucket_hosts:
                useless_hosts.extend(bucket_hosts)

        def _check_host_by_root():
            # check useless hosts
            bucket_hosts = client.bucket_get(root_name)
            crush_hosts.extend(bucket_hosts)
            for name, host in six.iteritems(hosts):
                if host['crush_name'] in bucket_hosts:
                    bucket_hosts.remove(host['crush_name'])
            useless_hosts.extend(bucket_hosts)

        def _check_osd_by_host():
            # add osd
            logger.info("start update osd crush resource")
            bucket_osds = []
            for name in crush_hosts:
                # get host from ceph
                _bucket_osds = client.bucket_get(name)
                logger.debug("get osd from crush(%s): %s",
                             name, _bucket_osds)
                bucket_osds.extend(_bucket_osds)
            for name, osd in six.iteritems(osds):
                if osd['name'] in bucket_osds:
                    bucket_osds.remove(osd['name'])
                    continue
            # add useless host to bucket_hosts
            if bucket_osds:
                useless_osds.extend(bucket_osds)

        if fault_domain == FaultDomain.DATACENTER:
            _check_dc_by_root()
            _check_rack_by_dc()
            _check_host_by_rack()
            _check_osd_by_host()

        # move rack to root
        if fault_domain == FaultDomain.RACK:
            _check_rack_by_root()
            _check_host_by_rack()
            _check_osd_by_host()

        # move host to root
        if fault_domain == FaultDomain.HOST:
            _check_host_by_root()
            _check_osd_by_host()

        # clean useless resource
        logger.info("start clean crush resource")
        logger.debug("remove osd resource: %s", useless_osds)
        for osd in useless_osds:
            client.bucket_remove(osd)
        logger.debug("remove host resource: %s", useless_hosts)
        for host in useless_hosts:
            client.bucket_remove(host)
        logger.debug("remove rack resource: %s", useless_racks)
        for rack in useless_racks:
            client.bucket_remove(rack)
        logger.debug("remove datacenter resource: %s", useless_dcs)
        for dc in useless_dcs:
            client.bucket_remove(dc)
        logger.info("crush update success")

    def pool_add_disk(self, pool, crush_content):
        logger.info("crush_content: %s", crush_content)
        root_name = crush_content.get('root_name')
        pool_name = pool.pool_name
        with RADOSClient(self.rados_args()) as client:
            if not client.pool_exists(pool_name):
                logger.warning("pool %s not exists", pool_name)
                return
            self._crush_rule_update(client, crush_content)

            pool_rep_size = client.get_pool_info(
                pool_name, 'size').get('size')
            pool_pg_num = client.get_pool_info(
                pool_name, 'pg_num').get('pg_num')
            total_osds = client.get_osds_by_bucket(root_name)
            new_pg_num = self._cal_pg_num(len(total_osds), pool_rep_size)
            max_split_count = int(client.conf_get(
                'mon_osd_max_split_count'))
            if new_pg_num > pool_pg_num and\
                    (new_pg_num / len(total_osds) <= max_split_count):
                logger.debug("will increate pg to {}".format(new_pg_num))
                client.set_pool_info(pool_name, 'pg_num', new_pg_num)
                client.set_pool_info(pool_name, 'pgp_num', new_pg_num)

    def pool_del_disk(self, pool, crush_content):
        logger.info("crush_content: %s", crush_content)
        with RADOSClient(self.rados_args()) as client:
            self._crush_rule_update(client, crush_content)

    def cluster_info(self):
        with RADOSClient(self.rados_args(), timeout='1') as rados_client:
            return rados_client.get_cluster_info()

    def update_pool(self, pool):
        rep_size = pool.replicate_size
        pool_name = pool.pool_name
        with RADOSClient(self.rados_args()) as client:
            if not client.pool_exists(pool_name):
                raise exc.PoolNameNotFound(pool=pool_name)
            client.pool_set_replica_size(pool_name=pool_name,
                                         rep_size=rep_size)

    def update_crush_policy(self, pools, crush_content):
        rule_name = crush_content.get('crush_rule_name')
        fault_domain = crush_content.get('fault_domain')
        root_name = crush_content.get('root_name')
        with RADOSClient(self.rados_args()) as client:
            self._crush_rule_update(client, crush_content)
            tmp_rule_name = "{}-new".format(rule_name)
            client.rule_rename(rule_name, tmp_rule_name)
            client.rule_add(rule_name, root_name, fault_domain)
            for pool in pools:
                pool_name = pool.pool_name
                if not client.pool_exists(pool_name):
                    raise exc.PoolNameNotFound(pool=pool_name)
                client.set_pool_info(pool_name, "crush_rule", rule_name)
            client.rule_remove(tmp_rule_name)
            return client.rule_get(rule_name)

    def osd_new(self, osd_fsid):
        with RADOSClient(self.rados_args()) as rados_client:
            return rados_client.osd_new(osd_fsid)

    def get_pools(self):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.get_pools()

    def get_pool_info(self, pool_name="rbd", keyword="size"):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.get_pool_info(pool_name, keyword)

    def _crush_tree_parse(self, nodes, root_name):
        datacenters = {}
        racks = {}
        hosts = {}
        osds = {}
        root = filter(
            lambda x: x['type'] == 'root' and x['name'] == root_name,
            nodes
        )
        parents = list(root)
        while True:
            if not parents:
                break
            parent = parents.pop()
            _nodes = filter(
                lambda x: x['id'] in parent['children'],
                nodes
            )
            for node in _nodes:
                if node['type'] == 'datacenter':
                    datacenters[node['name']] = []
                    parents.append(node)
                elif node['type'] == 'rack':
                    racks[node['name']] = []
                    parents.append(node)
                    if parent['name'] != root_name:
                        datacenters[parent['name']].append(node['name'])
                elif node['type'] == 'host':
                    hosts[node['name']] = []
                    parents.append(node)
                    if parent['name'] != root_name:
                        racks[parent['name']].append(node['name'])
                elif node['type'] == 'osd':
                    osds[node['name']] = None
                    hosts[parent['name']].append(node['name'])
                else:
                    raise ValueError("Item not expected: %s", node)
        return {
            "datacenters": datacenters,
            "racks": racks,
            "hosts": hosts,
            "osds": osds
        }

    def get_crush_rule_info(self, rule_name="replicated_rule"):
        with RADOSClient(self.rados_args(), timeout='5') as client:
            rule_info = client.get_crush_rule_info(rule_name)
            root_name = None
            fault_domain = None
            if "steps" in rule_info:
                for op_info in rule_info.get("steps"):
                    if op_info.get("op") == "take":
                        root_name = op_info.get("item_name")
                    elif op_info.get("op") == "chooseleaf_firstn":
                        fault_domain = op_info.get("type")
            crush_tree = client.crush_tree()
            nodes = crush_tree['nodes']
            info = self._crush_tree_parse(nodes, root_name)

            crush_rule_info = {
                "rule_name": rule_name,
                "root_name": root_name,
                "rule_id": rule_info.get("rule_id"),
                "type": fault_domain,
            }
            crush_rule_info.update(info)
            return crush_rule_info

    def get_bucket_info(self, bucket):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.bucket_get(bucket)

    def ceph_data_balance(self, action=None, mode=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.data_balance(action=action, mode=mode)

    def _is_paush_in_ceph(self, client):
        status = client.status()
        health = status.get("health", {})
        check = health.get("checks", {}).get("OSDMAP_FLAGS", {})
        summary = check.get('summary', {})
        message = summary.get('message')
        if message and "pause" in message:
            return True
        return False

    def _is_balancer_enable(self, client):
        # is module enable
        res = client.mgr_module_ls()
        if 'balancer' not in res["enabled_modules"]:
            return False
        # blancer status
        status = client.balancer_status()
        if not status["active"]:
            return False
        if "none" == status['mode']:
            return False
        return True

    def cluster_pause(self, enable=True):
        logger.info("cluster pause enable=%s", enable)
        with RADOSClient(self.rados_args(), timeout='5') as client:
            if enable:
                client.osd_pause()
                if self._is_paush_in_ceph(client):
                    logger.info("cluster pause success")
                    return True
                raise exc.ClusterPauseError()
            else:
                client.osd_unpause()
                if not self._is_paush_in_ceph(client):
                    logger.info("cluster unpause success")
                    return True
                raise exc.ClusterUnpauseError()

    def cluster_is_pause(self):
        logger.info("cluster is pause")
        with RADOSClient(self.rados_args(), timeout='5') as client:
            if self._is_paush_in_ceph(client):
                logger.info("cluster is pause")
                return True
            logger.info("cluster not pause")
            return False

    def cluster_status(self):
        logger.info("cluster status")
        with RADOSClient(self.rados_args(), timeout='5') as client:
            res = {
                "pause": self._is_paush_in_ceph(client),
                "balancer": self._is_balancer_enable(client)
            }
            logger.info("cluster status: %s", res)
            return res

    def mark_osds_out(self, osd_names):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            return rados_client.osd_out(osd_names)
