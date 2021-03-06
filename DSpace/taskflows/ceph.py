#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import json
import logging
import math
import os
import shutil

import six

from DSpace import exception as exc
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.objects.fields import ConfigKey
from DSpace.objects.fields import FaultDomain
from DSpace.objects.fields import PoolRole
from DSpace.objects.fields import PoolType
from DSpace.tools.base import Executor
from DSpace.tools.ceph import EC_POOL_RELATION_RE_POOL as ECP
from DSpace.tools.ceph import RADOSClient
from DSpace.tools.ceph import RBDProxy
from DSpace.tools.utils import change_erasure_pool_name
from DSpace.utils.coordination import synchronized

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

    def gen_config(self):
        logger.info("Ceph config directory '%s' create" % (self.conf_dir))
        self._generate_config_file()
        self._generate_admin_keyring()

    def clear_config(self):
        try:
            shutil.rmtree(self.conf_dir)
            logger.info("Directory '%s' has been removed successfully" % (
                self.conf_dir))
        except OSError as error:
            logger.warning(error)
            logger.info("Directory '%s' can not be removed" % (self.conf_dir))

    def ceph_admin_keyring(self):
        admin_keyring = objects.CephConfig.get_by_key(
            self.ctxt, 'keyring', 'client.admin')
        return admin_keyring

    def ceph_config(self):
        content = objects.ceph_config.ceph_config_content(
            self.ctxt, debug_config=False)
        return content

    def rados_args(self):
        mon_host = objects.ceph_config.ceph_config_get(
            self.ctxt, "global", "mon_host")
        res = {'mon_host': mon_host}
        admin_keyring = self.ceph_admin_keyring()
        if admin_keyring:
            res["keyring"] = self.key_file
        logger.info("ceph task, rados args: %s", res)
        return res

    def _enable_cephx(self):
        enable_cephx = objects.sysconfig.sys_config_get(
            self.ctxt, key=ConfigKey.ENABLE_CEPHX)
        return enable_cephx

    def enable_key_file_param(self):
        if self._enable_cephx():
            return ['-k', self.key_file]
        else:
            return []

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
        with RADOSClient(self.rados_args(), timeout='2') as client:
            return client.get_ceph_df()

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
        with RADOSClient(self.rados_args(), timeout='2') as client:
            return client.get_osd_df()

    """
    Get all osds in a pool
    """

    def get_pool_osds(self, pool_name):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            osds = client.get_osds_by_pool(pool_name)
            return osds

    """
    Get all osds in a host
    """

    def get_host_osds(self, host_name):
        pass

    def _calculate_weight(self, exist_buckets_id):
        # Calculate host weight
        for id, bucket in six.iteritems(exist_buckets_id):
            if "~" in bucket["name"]:
                continue
            if (bucket["type_name"] == "host") and (bucket["weight"] == 0):
                for item in bucket["items"]:
                    bucket["weight"] = int(bucket["weight"]) + \
                                       int(item["weight"])

        # Calculate rack weight
        for id, bucket in six.iteritems(exist_buckets_id):
            if "~" in bucket["name"]:
                continue
            if (bucket["type_name"] == "rack") and (bucket["weight"] == 0):
                for item in bucket["items"]:
                    weight = exist_buckets_id[item["id"]]["weight"]
                    item["weight"] = weight
                    bucket["weight"] = int(bucket["weight"]) + weight

        # Calculate datacenter weight
        for id, bucket in six.iteritems(exist_buckets_id):
            if "~" in bucket["name"]:
                continue
            if (bucket["type_name"] == "datacenter") and (
                    bucket["weight"] == 0):
                for item in bucket["items"]:
                    weight = exist_buckets_id[item["id"]]["weight"]
                    item["weight"] = weight
                    bucket["weight"] = int(bucket["weight"]) + weight

        # Calculate root weight
        for id, bucket in six.iteritems(exist_buckets_id):
            if "~" in bucket["name"]:
                continue
            if (bucket["type_name"] == "root") and (
                    bucket["weight"] == 0):
                for item in bucket["items"]:
                    weight = exist_buckets_id[item["id"]]["weight"]
                    item["weight"] = weight
                    bucket["weight"] = int(bucket["weight"]) + weight

        return exist_buckets_id

    @synchronized("crushmap_modify")
    def _add_osd_to_crushmap(self, client, hosts, osds):
        crushmap = client.get_crushmap()
        exist_devices = {}
        for device in crushmap["devices"]:
            exist_devices[device["id"]] = device
        crushmap["devices"] = []
        exist_buckets = {}
        for bucket in crushmap["buckets"]:
            exist_buckets[bucket["name"]] = bucket

        for name, host in six.iteritems(hosts):
            host_osds = [osds.get(osd_id) for osd_id in host['osds']]
            for osd in host_osds:
                if int(osd["id"]) not in exist_devices:
                    exist_devices[int(osd["id"])] = {
                        "id": osd["id"],
                        "name": "osd." + osd["id"],
                        "class": osd["disk_type"]
                    }
                else:
                    exist_devices[int(osd["id"])]["class"] = osd["disk_type"]
                exist_buckets[host['crush_name']]["items"].append({
                    "id": osd["id"],
                    "weight": 65536 * float(osd['size']) / (2 ** 40)
                })
        for id, device in six.iteritems(exist_devices):
            crushmap["devices"].append(device)

        exist_buckets_id = {}
        for name, bucket in six.iteritems(exist_buckets):
            exist_buckets_id[bucket["id"]] = bucket

        exist_buckets_id = self._calculate_weight(exist_buckets_id)
        crushmap["buckets"] = []
        for bucket_id, bucket in six.iteritems(exist_buckets_id):
            crushmap["buckets"].append(bucket)
        client.set_crushmap(crushmap)

    def _crush_rule_create(self, client, crush_content, pool_type,
                           extra_data=None):
        logger.info("rule create: %s", json.dumps(crush_content))
        rule_name = crush_content.get('crush_rule_name')
        default_root_name = crush_content.get('root_name')
        client.root_add(default_root_name)
        fault_domain = crush_content.get('fault_domain')
        datacenters = crush_content.get('datacenters')
        racks = crush_content.get('racks')
        hosts = crush_content.get('hosts')
        osds = crush_content.get('osds')
        # add datacenter
        if fault_domain == FaultDomain.DATACENTER:
            for name, dc in six.iteritems(datacenters):
                client.datacenter_add(dc['crush_name'])
                client.datacenter_move_to_root(
                    dc['crush_name'], default_root_name)
                dc_racks = [racks.get(rack_id)
                            for rack_id in dc['racks']]
                for rack in dc_racks:
                    client.rack_add(rack['crush_name'])
                    client.rack_move_to_datacenter(
                        rack['crush_name'], dc['crush_name'])
        # move rack to root
        if fault_domain == FaultDomain.RACK:
            for name, rack in six.iteritems(crush_content.get('racks')):
                client.rack_add(rack['crush_name'])
                client.rack_move_to_root(
                    rack['crush_name'], default_root_name)
        # add host
        if fault_domain in [FaultDomain.DATACENTER, FaultDomain.RACK]:
            for name, rack in six.iteritems(racks):
                rack_hosts = [hosts.get(name)
                              for name in rack['hosts']]
                for host in rack_hosts:
                    client.host_add(host['crush_name'])
                    client.host_move_to_rack(
                        host['crush_name'], rack['crush_name'])
        # move host to root
        if fault_domain in [FaultDomain.HOST, FaultDomain.OSD]:
            for name, host in six.iteritems(crush_content.get('hosts')):
                client.host_add(host['crush_name'])
                client.rack_move_to_root(
                    host['crush_name'], default_root_name)
        # add osd
        self._add_osd_to_crushmap(client, hosts, osds)
        # TODO osd

        client.rule_add(rule_name, default_root_name, fault_domain,
                        rule_type=pool_type, extra_data=extra_data)

    def pool_create(self, pool, can_specified_rep, crush_content):
        logger.debug("pool_data: %s", json.dumps(crush_content))
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
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
            data_chunk_num = pool.data_chunk_num
            coding_chunk_num = pool.coding_chunk_num
            extra_data = {'k': data_chunk_num, 'm': coding_chunk_num}
            if pool_name in client.pool_list():
                logger.error("pool %s alread exists", pool_name)
                raise exc.PoolExists(pool=pool_name)

            if pool_type == PoolType.ERASURE:
                size = data_chunk_num + coding_chunk_num
            else:
                size = rep_size
            pg_num = self._cal_pg_num(osd_num, size)
            logger.info('creating pool pg_num: %s', pg_num)
            self._crush_rule_create(client, crush_content, pool_type,
                                    extra_data)

            client.pool_create(pool_name=pool_name,
                               pool_type=pool_type,
                               rule_name=rule_name,
                               pg_num=pg_num,
                               pgp_num=pg_num,
                               rep_size=pool_rep_size)
            if pool_role == PoolRole.INDEX:
                client.set_pool_application(pool_name, "rgw")
            else:
                client.set_pool_application(pool_name, "rbd")
            if not can_specified_rep:
                client.pool_set_replica_size(
                    pool_name=pool_name, rep_size=rep_size)
            if pool_type == PoolType.ERASURE:
                client.pool_set_min_size(
                    pool_name=pool_name, min_size=data_chunk_num)

            return client.get_pool_stats(pool_name).get('pool_id')

    def config_set(self, cluster_temp_configs):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            for config in cluster_temp_configs:
                service = config['service']
                osd_list = None
                if service.startswith('osd'):
                    osd_id = service.split('.')[1]
                    if osd_id == '*':
                        osd_list = objects.OsdList.get_all(self.ctxt)
                client.config_set(service,
                                  config['key'],
                                  config['value'],
                                  osd_list)

    def rule_get(self, rule_name):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            rule_detail = client.rule_get(rule_name)
            return rule_detail

    def _cal_pg_num(self, osd_num, size=3):
        if not osd_num:
            return 0
        pg_num = (100 * osd_num) / size
        # mon_max_pg_per_osd
        if (pg_num * 3 / osd_num) >= 250:
            pg_num = 250 * (osd_num / 3)
        pg_log2 = int(math.floor(math.log(pg_num, 2)))
        logger.debug("osd_num: {}, size: {}, pg_log2: {}".format(
            osd_num, size, pg_log2))
        if pg_num > (2**16):
            pg_log2 = 16
        return 2**pg_log2

    def _crush_rule_delete(self, client, crush_content):
        logger.info("rule delete: %s", json.dumps(crush_content))
        rule_name = crush_content.get('crush_rule_name')
        root_name = crush_content.get('root_name')
        crush_rule_type = crush_content.get('crush_rule_type')
        # delete rule
        if crush_rule_type == PoolType.ERASURE:
            # ??????????????? ??????????????????crush_rule
            extra_re_rule_name = rule_name + ECP
            client.rule_remove(extra_re_rule_name)
        client.rule_remove(rule_name)
        # remove osd and host
        useless_queue = []
        parent_queue = [root_name]
        while len(parent_queue) > 0:
            parent = parent_queue.pop(0)
            logger.debug("parent is %s", parent)
            useless_queue.append(parent)
            items = client.bucket_get(parent)
            logger.debug("items is %s", items)
            if not items:
                continue
            for item in items:
                if item.startswith("osd."):
                    useless_queue.append(item)
                else:
                    parent_queue.append(item)
        useless_queue.reverse()
        for item in useless_queue:
            client.bucket_remove(item)
        logger.info("rule remove rule success")

    def pool_delete(self, pool):
        logger.debug("pool_delete, pool_data: %s", pool)
        pool_name = pool.pool_name
        pool_type = pool.type
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            client.pool_delete(pool_name, pool_type)

    def crush_delete(self, crush_content):
        logger.debug("crush_delete, data: %s", crush_content)
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            self._crush_rule_delete(client, crush_content)

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
        if fault_domain in [FaultDomain.HOST, FaultDomain.OSD]:
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
        if fault_domain in [FaultDomain.HOST, FaultDomain.OSD]:
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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            self._crush_rule_update(client, crush_content)

    def cluster_info(self):
        with RADOSClient(self.rados_args(), timeout='1') as client:
            return client.get_cluster_info()

    def update_pool(self, pool):
        rep_size = pool.replicate_size
        pool_name = pool.pool_name
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            if not client.pool_exists(pool_name):
                raise exc.PoolNameNotFound(pool=pool_name)
            client.pool_set_replica_size(pool_name=pool_name,
                                         rep_size=rep_size)
            if pool.role == 'gateway':
                for rgw_pool in self.rgw_pools:
                    if not client.pool_exists(rgw_pool):
                        raise exc.PoolNameNotFound(pool=rgw_pool)
                    client.pool_set_replica_size(pool_name=rgw_pool,
                                                 rep_size=rep_size)

    def update_crush_policy(self, pools, crush_content):
        rule_type = crush_content.get('crush_rule_type')
        rule_name = crush_content.get('crush_rule_name')
        fault_domain = crush_content.get('fault_domain')
        root_name = crush_content.get('root_name')
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            self._crush_rule_update(client, crush_content)
            tmp_rule_name = "{}-new".format(rule_name)
            client.rule_rename(rule_name, tmp_rule_name)
            client.rule_add(rule_name, root_name, fault_domain,
                            rule_type=rule_type)
            for pool in pools:
                pool_name = pool.pool_name
                if not client.pool_exists(pool_name):
                    raise exc.PoolNameNotFound(pool=pool_name)
                client.set_pool_info(pool_name, "crush_rule", rule_name)
                if pool.role == 'gateway':
                    for rgw_pool in self.rgw_pools:
                        if not client.pool_exists(rgw_pool):
                            raise exc.PoolNameNotFound(pool=rgw_pool)
                        client.set_pool_info(rgw_pool, "crush_rule", rule_name)
            client.rule_remove(tmp_rule_name)
            return client.rule_get(rule_name)

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

    @change_erasure_pool_name()
    def rbd_snap_create(self, pool_name, rbd_name, snap_name, pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_create(rbd_name, snap_name)

    def rbd_create(self, pool_name, rbd_name, rbd_size, pool_type=None):
        # rbd_size bytes
        if pool_type == PoolType.ERASURE:
            extra_pool_name = pool_name + ECP
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            if pool_type == PoolType.ERASURE and (extra_pool_name not in
                                                  rados_client.pool_list()):
                raise exc.PoolNameNotFound(pool=extra_pool_name)
            if pool_type == PoolType.REPLICATED:
                self.replicated_pool_rbd_create(
                    rados_client, pool_name, rbd_name, rbd_size)
            elif pool_type == PoolType.ERASURE:
                self.erasure_pool_rbd_create(
                    pool_name, extra_pool_name, rbd_name, rbd_size)

    def replicated_pool_rbd_create(self, rados_client, pool_name, rbd_name,
                                   rbd_size):
        with RBDProxy(rados_client, pool_name) as rbd_client:
            rbd_client.rbd_create(rbd_name, rbd_size)

    def erasure_pool_rbd_create(self, pool_name, extra_pool, rbd_name,
                                size_bytes):
        # rbd create --size 5T --data-pool ec-pool -p rep-pool rbd1
        # --size default_unit is MB
        size = int(size_bytes/(1024*1024))
        logger.debug('will by rbd cmd to create erasure volume:%s', rbd_name)
        shell_client = Executor()
        cmd = ['rbd', 'create', '--size', str(size), '--data-pool', pool_name,
               '-p', extra_pool, rbd_name, '-c', self.conf_file]
        cmd.extend(self.enable_key_file_param())
        rc, out, err = shell_client.run_command(cmd, timeout=5)
        if rc:
            raise exc.CephException(message=out)

    def rbd_remove(self, pool_name, rbd_name):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_remove(rbd_name)

    @change_erasure_pool_name()
    def rbd_delete(self, pool_name, rbd_name, pool_type=None):
        try:
            self.rbd_remove(pool_name, rbd_name)
        except exc.CephException:
            logger.info('will by rbd command to rm volume:%s', rbd_name)
            shell_client = Executor()
            rbd_param = '{}/{}'.format(pool_name, rbd_name)
            cmd = ['rbd', 'rm', rbd_param, '-c', self.conf_file]
            cmd.extend(self.enable_key_file_param())
            rc, out, err = shell_client.run_command(cmd, timeout=5)
            if rc:
                raise exc.CephException(message=out)

    def rbd_rename(self, pool_name, old_rbd_name, new_rbd_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_rename(old_rbd_name, new_rbd_name)

    def rbd_snap_remove(self, pool_name, rbd_name, snap_name):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_remove(rbd_name, snap_name)

    @change_erasure_pool_name()
    def rbd_snap_delete(self, pool_name, rbd_name, snap_name, pool_type=None):
        try:
            self.rbd_snap_remove(pool_name, rbd_name, snap_name)
        except exc.CephException:
            logger.info('will by rbd command to rm snapshot:%s', snap_name)
            shell_client = Executor()
            snap_param = '{}/{}@{}'.format(pool_name, rbd_name, snap_name)
            cmd = ['rbd', 'snap', 'rm', snap_param, '-c', self.conf_file]
            cmd.extend(self.enable_key_file_param())
            rc, out, err = shell_client.run_command(cmd, timeout=5)
            if rc:
                raise exc.CephException(message=out)

    def rbd_snap_rename(self, pool_name, rbd_name, old_name, new_name):
        with RADOSClient(self.rados_args()) as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_snap_rename(rbd_name, old_name, new_name)

    @change_erasure_pool_name(child_name_position=3)
    def rbd_clone_volume(self, p_p_name, p_v_name, p_s_name, c_p_anme,
                         c_v_name, pool_type=None, child_pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
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

    @change_erasure_pool_name()
    def rbd_protect_snap(self, pool_name, volume_name, snap_name,
                         pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.protect_snap(volume_name, snap_name)

    @change_erasure_pool_name()
    def rbd_unprotect_snap(self, pool_name, volume_name, snap_name,
                           pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_unprotect_snap(volume_name, snap_name)

    @change_erasure_pool_name()
    def rbd_flatten(self, c_p_name, c_v_name, pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if c_p_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=c_p_name)
            with RBDProxy(rados_client, c_p_name) as rbd_client:
                rbd_client.rbd_image_flatten(c_v_name)

    @change_erasure_pool_name()
    def rbd_rollback_to_snap(self, pool_name, v_name, s_name, pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_rollback_to_snap(v_name, s_name)

    @change_erasure_pool_name()
    def rbd_resize(self, pool_name, v_name, size, pool_type=None):
        with RADOSClient(self.rados_args(), timeout='5') as rados_client:
            if pool_name not in rados_client.pool_list():
                raise exc.PoolNameNotFound(pool=pool_name)
            with RBDProxy(rados_client, pool_name) as rbd_client:
                rbd_client.rbd_resize(v_name, size)

    def rbd_used_size(self, pool_name, rbd_name):
        shell_client = Executor()
        rbd_param = '{}/{}'.format(pool_name, rbd_name)
        cmd = ['rbd', 'du', rbd_param, '-c', self.conf_file, '--format',
               'json']
        cmd.extend(self.enable_key_file_param())
        rc, out, err = shell_client.run_command(cmd, timeout=5)
        if rc:
            raise exc.CephException(message=out)
        out = json.loads(out)
        image_and_snap = out['images']
        used_size = None
        for volume in image_and_snap:
            if not volume.get('snapshot'):
                used_size = volume.get('used_size')
        return used_size

    def osd_new(self, osd_fsid):
        with RADOSClient(self.rados_args()) as client:
            return client.osd_new(osd_fsid)

    def osd_remove_from_cluster(self, osd_name):
        with RADOSClient(self.rados_args()) as client:
            client.osd_down(osd_name)
            client.osd_out(osd_name)
            client.osd_crush_rm(osd_name)
            client.osd_rm(osd_name)
            client.auth_del(osd_name)

    def auth_get_key(self, entity):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.auth_get_key(entity)

    def get_pools(self):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.get_pools()

    def get_pool_info(self, pool_name="rbd", keyword="size"):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.get_pool_info(pool_name, keyword)

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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.bucket_get(bucket)

    def ceph_data_balance(self, action=None, mode=None):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.set_data_balance(action=action, mode=mode)

    def balancer_status(self):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.balancer_status()

    def is_module_enable(self, module_name):
        logger.info("get balancer module status")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            res = client.mgr_module_ls()
            if module_name not in res["enabled_modules"]:
                return False
            return True

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
        if not self.is_module_enable("balancer"):
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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
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
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            if self._is_paush_in_ceph(client):
                logger.info("cluster is pause")
                return True
            logger.info("cluster not pause")
            return False

    def cluster_status(self):
        logger.info("cluster status")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            res = {
                "created": True,
                "pause": self._is_paush_in_ceph(client),
                "balancer": self._is_balancer_enable(client)
            }
            logger.info("cluster status: %s", res)
            return res

    def mark_osds_out(self, osd_names):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.osd_out(osd_names)

    def osds_add_noout(self, osd_names):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.osds_add_noout(osd_names)

    def osds_rm_noout(self, osd_names):
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.osds_rm_noout(osd_names)

    def get_osd_tree(self):
        logger.info("Get osd tree info")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.get_osd_tree()

    def get_osd_stat(self):
        logger.info("Get ceph osd stat")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.get_osd_stat()

    def ceph_status_check(self):
        logger.debug("Check ceph cluster status")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.status()

    def osd_metadata(self, osd_id):
        logger.debug("get osd metadata")
        with RADOSClient(self.rados_args(), CONF.rados_timeout) as client:
            return client.osd_metadata(osd_id)
