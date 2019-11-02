import copy
import uuid

import six
from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.base import AdminBaseHandler
from t2stor.api.wsclient import WebSocketClientManager
from t2stor.i18n import _
from t2stor.objects import fields as s_fields
from t2stor.taskflows.ceph import CephTask

logger = logging.getLogger(__name__)


class PoolHandler(AdminBaseHandler):
    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        filters = filters or {}
        return objects.PoolList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def pool_get(self, ctxt, pool_id):
        return objects.Pool.get_by_id(ctxt, pool_id)

    def pool_osds_get(self, ctxt, pool_id):
        return objects.OsdList.get_by_pool(ctxt, pool_id)

    def _update_osd_crush_id(self, ctxt, osds, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.crush_rule_id = crush_rule_id
            osd.save()

    def _generate_osd_toplogy(self, ctxt, pool_id, osds):
        rack_dict = dict()
        host_dict = dict()
        crush_host_dict = dict()
        datacenter_dict = dict()
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            node = self.node_get(ctxt, osd.node_id)
            node_name = "pool{}-host{}".format(pool_id, node.hostname)
            rack = self.rack_get(ctxt, node.rack_id)
            rack_name = "pool{}-rack{}".format(pool_id, rack.name)
            datacenter = self.datacenter_get(ctxt, rack.datacenter_id)
            datacenter_name = "pool{}-dc{}".format(pool_id, datacenter.name)
            disk = self.disk_get(ctxt, osd.disk_id)
            osd_info = (osd.osd_id, disk.disk_size)
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
            "root_name": "pool{}-root".format(pool_id),
            "crush_rule_name": crush_rule_name
        }
        crush_content = copy.deepcopy(body)
        host_dict, rack_dict, datacenter_dict, crush_host_dict = (
            self._generate_osd_toplogy(ctxt, pool_id, osds))
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
            ec_profile = "plugin=jerasuer technique=reed_sol_van k={k} \
                          m={m}".format(k=k, m=m)
            body.update(ec_profile=ec_profile)
        return body, crush_content

    def _pool_create(self, ctxt, pool, osds):
        crush_rule_name = "rule-{}".format(pool.id)
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("create pool, body: {}".format(body))
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
        crush_rule = self.crush_rule_create(
            ctxt, crush_rule_name, pool.failure_domain_type, crush_content)
        pool.crush_rule_id = crush_rule.id
        pool.pool_id = db_pool_id
        pool.status = status
        crush_rule.rule_id = rule_id
        pool.save()
        crush_rule.save()
        self._update_osd_crush_id(ctxt, osds, crush_rule.id)
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=pool.cluster_id).get_client()
        wb_client.send_message(ctxt, pool, "CREATED", msg)

    def pool_create(self, ctxt, data):
        uid = str(uuid.uuid4())
        pool_name = "pool-{}".format(uid)
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
            replicated_size=data.get("replicated_size"),
            failure_domain_type=data.get("failure_domain_type"))
        pool.create()
        self._pool_create(ctxt, pool, osds)
        return pool

    def _pool_delete(self, ctxt, pool):
        nodes = []
        osds = []
        osd_ids = []
        racks = []
        datacenters = []
        crush_rule = self.crush_rule_get(ctxt, pool.crush_rule_id)
        toplogy_data = crush_rule.content
        logger.debug("get crush rule data: {}".format(toplogy_data))
        rule_name = toplogy_data.get("crush_rule_name")
        root_name = toplogy_data.get("root_name")
        pool_role = toplogy_data.get("pool_role")
        logger.error("crush_rule osds: {}".format(crush_rule.osds))
        for h, o in six.iteritems(toplogy_data.get('host')):
            nodes.append(h)
            for osd in o:
                oid, oname = osd.split('-')
                osd_name = "osd.{}".format(oname)
                osds.append(osd_name)
                osd_ids.append(oid)
        for r, h in six.iteritems(toplogy_data.get('rack')):
            racks.append(r)
        for d, r in six.iteritems(toplogy_data.get('datacenter')):
            datacenters.append(d)
        data = {
            "osds": osds,
            "nodes": nodes,
            "racks": racks,
            "datacenters": datacenters,
            "root_name": root_name,
            "pool_role": pool_role
        }
        logger.debug("pool delete: {}".format(data))
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
        self._update_osd_crush_id(ctxt, osd_ids, None)
        pool.crush_rule_id = None
        pool.osd_num = None
        pool.stats = status
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=pool.cluster_id).get_client()
        wb_client.send_message(ctxt, pool, "CREATED", msg)

    def pool_delete(self, ctxt, pool_id):
        pool = self.pool_get(ctxt, pool_id)
        self._pool_delete(ctxt, pool)
        pool.save()
        pool.destroy()
        return pool

    def _pool_increase_disk(self, ctxt, pool, osds):
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("increase disk body: {}".format(body))
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_add_disk(body)
            msg = _("{} increase disk success").format(pool.pool_name)
            pool.osd_num += len(osds)
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} increase disk error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
        crush_rule = self.crush_rule_get(ctxt, pool.crush_rule_id)
        crush_osds = [osd.id for osd in crush_rule.osds]
        new_osds = crush_osds + osds
        logger.debug("pool increate disk, new_osds: {}".format(new_osds))
        _tmp, content = self._generate_pool_opdata(ctxt, pool, new_osds)
        crush_rule.content = content
        logger.debug("crush_rule content{}".format(crush_rule.content))
        crush_rule.save()
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=pool.cluster_id).get_client()
        wb_client.send_message(ctxt, pool, "INCREASE_DISK", msg)

    def pool_increase_disk(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(ctxt, id)
        osds = data.get('osds')
        self._pool_increase_disk(ctxt, pool, osds)
        pool.save()
        return pool

    def _pool_decrease_disk(self, ctxt, pool, osds):
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("decrease disk body: {}".format(body))
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_del_disk(body)
            msg = _("{} decrease disk success").format(pool.pool_name)
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} decrease disk error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
        crush_rule = self.crush_rule_get(ctxt, pool.crush_rule_id)
        crush_osds = [osd.id for osd in crush_rule.osds]
        new_osds = list(set(crush_osds).difference(set(osds)))
        _tmp, content = self._generate_pool_opdata(ctxt, pool, new_osds)

        crush_rule.content = content
        logger.debug("crush_content {}".format(crush_rule.content))
        crush_rule.save()
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=pool.cluster_id).get_client()
        wb_client.send_message(ctxt, pool, "DECREASE_DISK", msg)

    def pool_decrease_disk(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(ctxt, id)
        osds = data.get('osds')
        self._pool_decrease_disk(ctxt, pool, osds)
        pool.save()
        return pool

    def pool_update_display_name(self, ctxt, id, name):
        pool = objects.Pool.get_by_id(ctxt, id)
        pool.display_name = name
        pool.save()
        return pool

    def pool_update_policy(self, ctxt, id, data):
        pool = objects.Pool.get_by_id(ctxt, id)
        rep_size = data.get('rep_size')
        fault_domain = data.get('fault_domain')
        pool.failure_domain_type = fault_domain
        pool.replicate_size = rep_size
        crush_rule = self.crush_rule_get(ctxt, pool.crush_rule_id)
        osds = [osd.id for osd in crush_rule.osds]
        body, crush_content = self._generate_pool_opdata(ctxt, pool, osds)
        logger.debug("pool policy, body: {}".format(body))
        logger.debug("pool policy, crush content: {}".format(crush_content))
        try:
            ceph_client = CephTask(ctxt)
            new_rule_id = ceph_client.update_pool_policy(body).get('rule_id')
            crush_rule.rule_id = new_rule_id
            msg = _("{} update policy success").format(pool.pool_name)
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} update policy error").format(pool.pool_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
            return None
        crush_rule.content = crush_content
        crush_rule.save()
        pool.save()
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=pool.cluster_id).get_client()
        wb_client.send_message(ctxt, pool, "UPDATE_POLICY", msg)
        return pool
