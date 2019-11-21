import copy
import json
import uuid

import six
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.taskflows.ceph import CephTask
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class PoolHandler(AdminBaseHandler):
    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None, tab=None):
        pools = objects.PoolList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

        if tab == 'default':
            for pool in pools:
                prometheus = PrometheusTool(ctxt)
                pool.metrics = {}
                prometheus.pool_get_capacity(pool)
                prometheus.pool_get_pg_state(pool)
                pg_state = pool.metrics.get("pg_state")
                time_now = timeutils.utcnow(with_timezone=True)
                if pool.updated_at:
                    time_diff = time_now - pool.created_at
                    logger.info("pool %s, get metrics: %s",
                                pool.pool_name, pool.metrics)
                    if time_diff.total_seconds() <= 60:
                        continue
                    if pool.status not in [s_fields.PoolStatus.CREATING,
                                           s_fields.PoolStatus.DELETING,
                                           s_fields.PoolStatus.ERROR,
                                           s_fields.PoolStatus.DELETED]:
                        if (pg_state and pg_state.get("healthy") < 1.0 and
                                pool.status != s_fields.PoolStatus.INACTIVE):
                            pool.status = s_fields.PoolStatus.INACTIVE
                            pool.save()
                        if (pg_state and pg_state.get("healthy") >= 1.0 and
                                pool.status != s_fields.PoolStatus.ACTIVE):
                            pool.status = s_fields.PoolStatus.ACTIVE
                            pool.save()
        if tab == 'io':
            prometheus = PrometheusTool(ctxt)
            for pool in pools:
                pool.metrics = {}
                prometheus.pool_get_perf(pool)

        return pools

    def pool_get_count(self, ctxt, filters=None):
        return objects.PoolList.get_count(ctxt, filters=filters)

    def pool_get(self, ctxt, pool_id, expected_attrs=None):
        return objects.Pool.get_by_id(
            ctxt, pool_id, expected_attrs=expected_attrs)

    def pool_osds_get(self, ctxt, pool_id, expected_attrs=None):
        osds = objects.OsdList.get_by_pool(
            ctxt, pool_id, expected_attrs=expected_attrs)
        prometheus = PrometheusTool(ctxt)
        for osd in osds:
            osd.metrics = {}
            prometheus.osd_get_capacity(osd)
        return osds

    def _update_osd_info(self, ctxt, osds, osd_status, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.status = osd_status
            osd.crush_rule_id = crush_rule_id
            osd.save()

    def _generate_osd_toplogy(self, ctxt, pool, osds):
        rack_dict = dict()
        host_dict = dict()
        crush_host_dict = dict()
        datacenter_dict = dict()
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            node = self.node_get(ctxt, osd.node_id)
            node_name = "{}-{}".format(pool.pool_name, node.hostname)
            rack = self.rack_get(ctxt, node.rack_id)
            rack_name = "{}-rack{}".format(pool.pool_name, rack.id)
            datacenter = self.datacenter_get(ctxt, rack.datacenter_id)
            datacenter_name = "{}-datacenter{}".format(pool.pool_name,
                                                       datacenter.id)
            disk = self.disk_get(ctxt, osd.disk_id)
            osd_info = (osd.osd_id, disk.size)
            crush_osd_info = "{}-{}".format(osd_id, osd.osd_id)
            if not rack_dict.get(rack_name):
                rack_dict[rack_name] = [node_name]
            if rack_dict[rack_name] and node_name not in rack_dict[rack_name]:
                rack_dict[rack_name].append(node_name)
            if not host_dict.get(node_name):
                host_dict[node_name] = [osd_info]
                crush_host_dict[node_name] = [crush_osd_info]
            if host_dict[node_name] and osd_info not in host_dict[node_name]:
                host_dict[node_name].append(osd_info)
                crush_host_dict[node_name].append(crush_osd_info)
            if not datacenter_dict.get(datacenter_name):
                datacenter_dict[datacenter_name] = [rack_name]
            if (datacenter_dict[datacenter_name] and
                    rack_name not in datacenter_dict[datacenter_name]):
                datacenter_dict[datacenter_name].append(rack_name)
        return host_dict, rack_dict, datacenter_dict, crush_host_dict

    def _generate_pool_opdata(self, ctxt, pool, osds):
        pool_id = pool.id
        crush_rule_name = "rule-{}".format(pool_id)
        body = {
            "pool_name": pool.pool_name,
            "pool_type": pool.type,
            "pool_role": pool.role,
            "rep_size": pool.replicate_size,
            "fault_domain": pool.failure_domain_type,
            "root_name": "{}-root".format(pool.pool_name),
            "crush_rule_name": crush_rule_name
        }
        crush_content = copy.deepcopy(body)
        host_dict, rack_dict, datacenter_dict, crush_host_dict = (
            self._generate_osd_toplogy(ctxt, pool, osds))
        logger.debug("*** _generate_pool_opdata: {} {} {} {}".format(
            host_dict, rack_dict, datacenter_dict, crush_host_dict))
        if pool.failure_domain_type == 'host':
            body.update(host=host_dict)
            crush_content.update(host=crush_host_dict)
        if pool.failure_domain_type == 'rack':
            body.update(host=host_dict, rack=rack_dict)
            crush_content.update(host=crush_host_dict, rack=rack_dict)
        if pool.failure_domain_type == 'datacenter':
            body.update(
                datacenter=datacenter_dict, rack=rack_dict, host=host_dict)
            crush_content.update(
                datacenter=datacenter_dict, rack=rack_dict,
                host=crush_host_dict)
        if pool.data_chunk_num and pool.coding_chunk_num:
            k = str(pool.data_chunk_num)
            m = str(pool.coding_chunk_num)
            body.update(k=k)
            body.update(m=m)
        return body, crush_content

    def _pool_create(self, ctxt, pool, osds):
        crush_rule_name = "rule-{}".format(pool.id)
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        ceph_version = objects.sysconfig.sys_config_get(ctxt, 'ceph_version')
        if (ceph_version == s_fields.CephVersion.T2STOR):
            logger.info("ceph version is: %s, can specified replicate size "
                        "while creating pool", ceph_version)
            # can specified replicate size
            body.update(specified_rep=True)
        else:
            logger.info("ceph version is: %s, can't specified replicate size "
                        "while creating pool", ceph_version)
            body.update(specified_rep=False)
        crush_rule = self.crush_rule_create(
            ctxt, crush_rule_name, pool.failure_domain_type, crush_content)
        self._update_osd_info(ctxt, osds, s_fields.OsdStatus.ACTIVE,
                              crush_rule.id)
        pool.crush_rule_id = crush_rule.id
        pool.save()
        logger.debug("create pool, body: %s", body)
        try:
            ceph_client = CephTask(ctxt)
            db_pool_id = ceph_client.pool_create(body)
            rule_id = ceph_client.rule_get(crush_rule_name).get('rule_id')
            status = s_fields.PoolStatus.ACTIVE
            msg = _("create pool success")
        except exception.StorException as e:
            logger.error("create pool error: {}".format(e))
            db_pool_id = None
            rule_id = None
            status = s_fields.PoolStatus.ERROR
            msg = _("create pool error")
        crush_rule.rule_id = rule_id
        crush_rule.save()
        pool.pool_id = db_pool_id
        pool.status = status
        pool.save()
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, pool, "CREATED", msg)

    def pool_create(self, ctxt, data):
        uid = str(uuid.uuid4())
        pool_name = "pool-{}".format(uid.replace('-', ''))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.CREATE)
        osds = data.get('osds')
        pool = objects.Pool(
            ctxt,
            cluster_id=ctxt.cluster_id,
            status=s_fields.PoolStatus.CREATING,
            pool_name=pool_name,
            display_name=data.get("name"),
            type=data.get("type"),
            role=data.get("role"),
            data_chunk_num=data.get("data_chunk_num"),
            coding_chunk_num=data.get("coding_chunk_num"),
            osd_num=len(osds),
            speed_type=data.get("speed_type"),
            replicate_size=data.get("replicate_size"),
            failure_domain_type=data.get("failure_domain_type"),
            data_pool=data.get("data_pool")
        )
        pool.create()
        self._pool_create(ctxt, pool, osds)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        return pool

    def _pool_delete(self, ctxt, pool):
        nodes = []
        osds = []
        osd_ids = []
        racks = []
        datacenters = []
        crush_rule = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                 expected_attrs=['osds'])
        toplogy_data = crush_rule.content
        logger.debug("pool delete, get crush rule data: %s", toplogy_data)
        rule_name = toplogy_data.get("crush_rule_name")
        root_name = toplogy_data.get("root_name")
        pool_role = toplogy_data.get("pool_role")
        failure_domain = toplogy_data.get("fault_domain")
        crush_rule_name = toplogy_data.get("crush_rule_name")
        pool_name = toplogy_data.get("pool_name")
        if pool_name != pool.pool_name:
            logger.error("pool name don't match %s:<%s>", pool.pool_name,
                         pool_name)
            raise exception.ProgrammingError(reason="pool name don't match")
        logger.debug("crush_rule osds: {}".format(crush_rule.osds))
        for h, o in six.iteritems(toplogy_data.get('host')):
            nodes.append(h)
            for osd in o:
                oid, oname = osd.split('-')
                osd_name = "osd.{}".format(oname)
                osds.append(osd_name)
                osd_ids.append(oid)

        if failure_domain == "rack":
            for r, h in six.iteritems(toplogy_data.get('rack')):
                if r:
                    racks.append(r)
        if failure_domain == "datacenter":
            for d, r in six.iteritems(toplogy_data.get('datacenter')):
                if d:
                    datacenters.append(d)
                    for rack in r:
                        racks.append(rack)
        data = {
            "osds": osds,
            "nodes": nodes,
            "racks": racks,
            "datacenters": datacenters,
            "root_name": root_name,
            "pool_role": pool_role,
            "crush_rule_name": crush_rule_name,
            "pool_name": pool_name
        }
        logger.info("delete pool: %s, send data to client: %s", pool_name,
                    json.dumps(data))
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_delete(data)
            # 不删除默认的replicated_rule
            if rule_name not in 'replicated_rule':
                pass
            if not crush_rule.osds:
                ceph_client.rule_remove()
            status = s_fields.PoolStatus.DELETED
            msg = _("delete pool success")
        except exception.StorException as e:
            logger.error("create pool error: {}".format(e))
            status = s_fields.PoolStatus.ERROR
            msg = _("delete pool error")
        if rule_name not in 'replicated_rule':
            self.crush_rule_delete(ctxt, pool.crush_rule_id)
        self._update_osd_info(ctxt, osd_ids, s_fields.OsdStatus.AVAILABLE,
                              None)
        pool.crush_rule_id = None
        pool.osd_num = None
        pool.stats = status
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, pool, "DELETED", msg)

    def pool_delete(self, ctxt, pool_id):
        pool = self.pool_get(ctxt, pool_id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.DELETE)
        self._pool_delete(ctxt, pool)
        pool.save()
        pool.destroy()
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        return pool

    def _pool_increase_disk(self, ctxt, pool, osds):
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("increase disk body: {}".format(body))
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_add_disk(body)
            msg = _("{} increase disk success").format(pool.pool_name)
            pool.osd_num += len(osds)
            pool.status = s_fields.PoolStatus.ACTIVE
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} increase disk error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
        crush_rule = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                 expected_attrs=['osds'])
        crush_osds = [osd.id for osd in crush_rule.osds]
        new_osds = crush_osds + osds
        logger.debug("pool increate disk, new_osds: {}".format(new_osds))
        _tmp, content = self._generate_pool_opdata(ctxt, pool, new_osds)
        crush_rule.content = content
        logger.debug("crush_rule content{}".format(crush_rule.content))
        crush_rule.save()
        self._update_osd_info(ctxt, osds, s_fields.OsdStatus.ACTIVE,
                              crush_rule.id)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, pool, "INCREASE_DISK", msg)

    def pool_increase_disk(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(ctxt, id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_ADD_DISK)
        osds = data.get('osds')
        self._pool_increase_disk(ctxt, pool, osds)
        pool.save()
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        return pool

    def _pool_decrease_disk(self, ctxt, pool, osds):
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("decrease disk body: {}".format(body))
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_del_disk(body)
            pool.status = s_fields.PoolStatus.ACTIVE
            msg = _("{} decrease disk success").format(pool.pool_name)
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} decrease disk error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
        crush_rule = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                 expected_attrs=['osds'])
        crush_osds = [osd.id for osd in crush_rule.osds]
        new_osds = list(set(crush_osds).difference(set(osds)))
        _tmp, content = self._generate_pool_opdata(ctxt, pool, new_osds)

        crush_rule.content = content
        logger.debug("crush_content {}".format(crush_rule.content))
        crush_rule.save()
        self._update_osd_info(ctxt, osds, s_fields.OsdStatus.AVAILABLE, None)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, pool, "DECREASE_DISK", msg)

    def _get_osd_fault_domains(self, ctxt, fault_domain, osd_ids):
        nodes = []
        racks = []
        datacenters = []
        for osd_id in osd_ids:
            osd = objects.Osd.get_by_id(ctxt, osd_id)
            node = objects.Node.get_by_id(ctxt, osd.node_id)
            if node not in nodes:
                nodes.append(node)
            rack = objects.Rack.get_by_id(ctxt, node.rack_id)
            if rack not in racks:
                racks.append(rack)
            datacenter = objects.Datacenter.get_by_id(ctxt, rack.datacenter_id)
            if datacenter not in datacenters:
                datacenters.append(datacenter)
        if fault_domain == "host":
            logger.info("%s, get hosts: %s", osd_id, nodes)
            return len(nodes)
        elif fault_domain == "rack":
            logger.info("%s, get racks: %s", osd_id, racks)
            return len(racks)
        elif fault_domain == "datacenter":
            logger.info("%s, get datacenters: %s", osd_id, datacenters)
            return len(datacenters)

    def _check_data_lost(self, ctxt, pool, osd_ids):
        fault_domain = pool.failure_domain_type
        rep_size = pool.replicate_size
        pool_osd_ids = [i.id for i in pool.osds]
        pool_avail_domain = self._get_osd_fault_domains(
            ctxt, fault_domain, pool_osd_ids)
        dec_domain = self._get_osd_fault_domains(ctxt, fault_domain, osd_ids)
        if dec_domain >= rep_size:
            logger.error("can't remove osds more than pool's rep size: %s",
                         rep_size)
            raise exception.InvalidInput(
                reason="can't remove osds more than pool's rep size")
        if dec_domain >= pool_avail_domain:
            logger.error("can't remove osds more than pool's available domain")
            raise exception.InvalidInput(
                reason="can't remove osds more than pool's available domain")

    def pool_decrease_disk(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(
            ctxt, id, expected_attrs=['crush_rule', 'osds'])
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_DEL_DISK)
        osds = data.get('osds')
        self._check_data_lost(ctxt, pool, osds)
        self._pool_decrease_disk(ctxt, pool, osds)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        pool.save()
        return pool

    def pool_update_display_name(self, ctxt, id, name):
        pool = objects.Pool.get_by_id(ctxt, id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.UPDATE)
        logger.info("update pool name from %s to %s", pool.display_name, name)
        pool.display_name = name
        pool.save()
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        return pool

    def pool_update_policy(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(ctxt, id)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_UPDATE_POLICY)
        rep_size = data.get('replicate_size')
        fault_domain = data.get('failure_domain_type')
        if int(rep_size) < int(pool.replicate_size):
            logger.error("can't set pool size from %s to %s",
                         pool.replicate_size, rep_size)
            raise exception.InvalidInput(reason="can't set lower rep size")
        if pool.failure_domain_type == "rack" and fault_domain == "host":
            logger.error("can't set fault_domain from rack to host")
            raise exception.InvalidInput(
                reason="can't set fault_domain from rack to host")
        pool.failure_domain_type = fault_domain
        pool.replicate_size = rep_size
        crush_rule = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                 expected_attrs=["osds"])
        osds = [osd.id for osd in crush_rule.osds]
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("pool policy, body: {}".format(body))
        logger.debug("pool policy, crush content: {}".format(crush_content))
        try:
            ceph_client = CephTask(ctxt)
            new_rule_id = ceph_client.update_pool_policy(body).get('rule_id')
            crush_rule.rule_id = new_rule_id
            msg = _("{} update policy success").format(pool.pool_name)
            status = s_fields.PoolStatus.ACTIVE
        except exception.StorException as e:
            logger.error("update pool policy error: {}".format(e))
            msg = _("{} update policy error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
            return None
        crush_rule.content = crush_content
        crush_rule.save()
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.pool_name,
                           resource_data=objects.json_encode(pool))
        pool.save()
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, pool, "UPDATE_POLICY", msg)
        return pool

    def pool_fact_total_size_bytes(self, ctxt, pool_id):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        pool_name = pool.pool_name
        # get_fact_pool_size
        cluster_info = self.ceph_cluster_info(ctxt)
        pool_list = cluster_info.get('pool_list', [])
        for p_pool in pool_list:
            if p_pool['name'] == pool_name:
                is_avaible = p_pool['stats']['max_avail']
                size = (p_pool['stats']['bytes_used'] +
                        p_pool['stats']['max_avail'])
                return {
                    'is_avaible': True if is_avaible > 0 else False,
                    'size': size
                }
        return {}

    def _generate_osd_tree(self, ctxt, osds):
        node_ids = {}
        for osd in osds:
            if osd.node_id not in node_ids:
                node_ids[osd.node_id] = []
            node_ids[osd.node_id].append(osd)

        # nodes
        nodes = objects.NodeList.get_all(ctxt)
        rack_ids = {}
        for node in nodes:
            if node.rack_id not in rack_ids:
                rack_ids[node.rack_id] = []
            rack_ids[node.rack_id].append(node)
            node.osds = node_ids.get(node.id, [])

        # racks
        racks = objects.RackList.get_all(ctxt)
        dc_ids = {}
        for rack in racks:
            if rack.datacenter_id not in dc_ids:
                dc_ids[rack.datacenter_id] = []
            dc_ids[rack.datacenter_id].append(rack)
            rack.nodes = rack_ids.get(rack.id, [])

        # datacenters
        dcs = objects.DatacenterList.get_all(ctxt)
        for dc in dcs:
            dc.racks = dc_ids.get(dc.id, [])
        if not dcs and racks:
            return racks
        return dcs

    def pool_osd_tree(self, ctxt, pool_id):
        pool = objects.Pool.get_by_id(ctxt, pool_id)
        if not pool:
            logger.error("can't get specified pool by pool_id: ", pool_id)
            raise exception.PoolNotFound(pool_id=pool_id)
        crush_rule = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                 expected_attrs=['osds'])
        osds = crush_rule.osds
        logger.debug("crush rule %s, get osds: %s", crush_rule.rule_name, osds)
        if not osds:
            logger.error("can't get osds from crush rule: %s",
                         crush_rule.rule_name)
            raise exception.ProgrammingError(
                reason="can't get osds from crush rule: {}".format(
                    crush_rule.rule_name))
        osd_tree = self._generate_osd_tree(ctxt, osds)
        return osd_tree
