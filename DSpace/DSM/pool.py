import json
import uuid

from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects import utils as obj_utils
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.crush import CrushContentGen
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class PoolHandler(AdminBaseHandler):
    def _pool_update_metrics(self, ctxt, pool):
        prometheus = PrometheusTool(ctxt)
        prometheus.pool_get_capacity(pool)
        prometheus.pool_get_pg_state(pool)
        pg_state = pool.metrics.get("pg_state")
        if pool.updated_at:
            logger.info("pool %s, get metrics: %s",
                        pool.pool_name, json.dumps(pool.metrics))
            if pool.status not in [s_fields.PoolStatus.CREATING,
                                   s_fields.PoolStatus.DELETING,
                                   s_fields.PoolStatus.DELETED]:
                if pg_state:
                    pg_unactive = pg_state.get("unactive")
                    pg_degraded = pg_state.get("degraded")
                    pg_recovering = pg_state.get("recovering")
                    pg_healthy = pg_state.get("healthy")
                    if pg_unactive and pg_unactive > 0:
                        pool.status = s_fields.PoolStatus.WARNING
                    elif pg_degraded and pg_degraded > 0:
                        pool.status = s_fields.PoolStatus.DEGRADED
                    elif pg_recovering and pg_recovering > 0:
                        pool.status = s_fields.PoolStatus.RECOVERING
                    elif pg_healthy and pg_healthy == 1:
                        pool.status = s_fields.PoolStatus.ACTIVE
                    else:
                        pool.status = s_fields.PoolStatus.WARNING
                pool.save()

    def _filter_by_role(self, pools):
        ps = []
        for pool in pools:
            if pool.role != s_fields.PoolRole.OBJECT_META:
                ps.append(pool)
        return ps

    def pool_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None, tab=None):
        pools = objects.PoolList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

        pools = self._filter_by_role(pools)

        if tab == 'default':
            for pool in pools:
                if not pool.need_metrics():
                    continue
                self._pool_update_metrics(ctxt, pool)
        if tab == 'io':
            prometheus = PrometheusTool(ctxt)
            for pool in pools:
                if not pool.need_metrics():
                    continue
                prometheus.pool_get_perf(pool)

        return pools

    def pool_get_count(self, ctxt, filters=None):
        return objects.PoolList.get_count(ctxt, filters=filters)

    def pool_get(self, ctxt, pool_id, expected_attrs=None):
        pool = objects.Pool.get_by_id(
            ctxt, pool_id, expected_attrs=expected_attrs)
        self._osds_update_size(ctxt, pool.osds)
        for osd in pool.osds:
            osd.node = objects.Node.get_by_id(ctxt, osd.node_id)
        return pool

    def pool_osds_get(self, ctxt, pool_id, expected_attrs=None):
        osds = objects.OsdList.get_by_pool(
            ctxt, pool_id, expected_attrs=expected_attrs)
        prometheus = PrometheusTool(ctxt)
        for osd in osds:
            prometheus.osd_get_capacity(osd)
        return osds

    def _update_osd_info(self, ctxt, osds, crush_rule_id):
        for osd in osds:
            if isinstance(osd, int):
                osd = objects.Osd.get_by_id(ctxt, osd)
            elif isinstance(osd, str):
                osd = objects.Osd.get_by_osd_id(ctxt, osd)
            osd.crush_rule_id = crush_rule_id
            osd.save()

    def _pool_create(self, ctxt, pool, data):
        # pool db create
        # crush db create
        # crush data create
        # create pool
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.CREATE)
        try:
            osd_ids = data.get('osds')
            osds = objects.OsdList.get_all(ctxt, filters={"id": osd_ids})
            crush_rule = obj_utils.rule_create(
                ctxt, rule_type=pool.failure_domain_type)
            gen = CrushContentGen(
                ctxt, crush_rule.rule_name, crush_rule.rule_name,
                fault_domain=data.get("failure_domain_type"), osds=osds)
            crush_rule.content = gen.gen_content()
            crush_rule.save()
            for osd in osds:
                osd.crush_rule_id = crush_rule.id
                osd.status = s_fields.OsdStatus.ACTIVE
                osd.save()
            pool.crush_rule_id = crush_rule.id
            pool.save()

            ceph_version = objects.sysconfig.sys_config_get(
                ctxt, 'ceph_version_name')
            if ceph_version == s_fields.CephVersion.T2STOR:
                logger.info("ceph version is: %s, can specified replicate"
                            " size while creating pool", ceph_version)
                # can specified replicate size
                specified_rep = True
            else:
                logger.info("ceph version is: %s, can't specified replicate"
                            " size while creating pool", ceph_version)
                specified_rep = False

            ceph_client = CephTask(ctxt)
            db_pool_id = ceph_client.pool_create(pool, specified_rep,
                                                 crush_rule.content)
            rule_id = ceph_client.rule_get(crush_rule.rule_name).get('rule_id')
            crush_rule.rule_id = rule_id
            crush_rule.save()
            status = s_fields.PoolStatus.ACTIVE
            msg = _("create pool success: {}").format(pool.display_name)
            op_status = "CREATE_SUCCESS"
            err_msg = None
        except Exception as e:
            logger.exception("create pool error: %s", e)
            err_msg = str(e)
            db_pool_id = None
            rule_id = None
            status = s_fields.PoolStatus.ERROR
            msg = _("create pool error: {}").format(pool.display_name)
            op_status = "CREATE_ERROR"
        pool.pool_id = db_pool_id
        pool.status = status
        pool.save()
        self.send_websocket(ctxt, pool, op_status, msg)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool, status=status, err_msg=err_msg)

    def _check_pool_display_name(self, ctxt, display_name):
        filters = {"display_name": display_name}
        pools = objects.PoolList.get_all(ctxt, filters=filters)
        if pools:
            logger.error("pool display_name duplicate: %s", display_name)
            raise exception.PoolExists(pool=display_name)

    def pool_create(self, ctxt, data):
        self.check_mon_host(ctxt)
        uid = str(uuid.uuid4())
        pool_display_name = data.get("name")
        self._check_pool_display_name(ctxt, pool_display_name)
        pool_name = "pool-{}".format(uid.replace('-', ''))
        osds = data.get('osds')
        pool = objects.Pool(
            ctxt,
            cluster_id=ctxt.cluster_id,
            status=s_fields.PoolStatus.CREATING,
            pool_name=pool_name,
            display_name=pool_display_name,
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
        objects.sysconfig.sys_config_set(ctxt, 'pool_undo', {})
        self.task_submit(self._pool_create, ctxt, pool, data)
        return pool

    def _crush_rule_delete(self, ctxt, crush):
        pools = objects.PoolList.get_all(
            ctxt, filters={'crush_rule_id': crush.id})
        if len(pools) > 1:
            return

        ceph_client = CephTask(ctxt)
        ceph_client.crush_delete(crush.content)
        self.crush_rule_delete(ctxt, crush.id)
        osds = objects.OsdList.get_all(
            ctxt, filters={"crush_rule_id": crush.id})
        self._update_osd_info(
            ctxt, osds, None)

    def _pool_delete(self, ctxt, pool, begin_action):
        crush_rule = pool.crush_rule
        try:
            ceph_client = CephTask(ctxt)
            ceph_client.pool_delete(pool)
            status = s_fields.PoolStatus.DELETED
            self._crush_rule_delete(ctxt, crush_rule)
            msg = _("delete pool success: {}").format(pool.display_name)
            op_status = "DELETE_SUCCESS"
            pool.crush_rule_id = None
            pool.osd_num = None
            err_msg = None
        except exception.StorException as e:
            logger.error("delete pool error: %s", e)
            err_msg = str(e)
            status = s_fields.PoolStatus.ERROR
            msg = _("delete pool error: {}").format(pool.display_name)
            op_status = "DELETE_ERROR"
        pool.status = status
        pool.save()
        if pool.status != s_fields.PoolStatus.ERROR:
            pool.destroy()
        self.send_websocket(ctxt, pool, op_status, msg)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool, status=status, err_msg=err_msg)

    def _check_pool_status(self, ctxt, pool):
        if pool.status in [s_fields.PoolStatus.CREATING,
                           s_fields.PoolStatus.PROCESSING,
                           s_fields.PoolStatus.DELETING]:
            raise exception.InvalidInput(_("Pool %s is in processing, "
                                           "please wait") % pool.display_name)

        pool_id = objects.sysconfig.sys_config_get(
            ctxt, s_fields.ConfigKey.OBJECT_META_POOL)
        if pool_id == pool.id:
            raise exception.Invalid(_("Pool %s is used by object store!")
                                    % pool.display_name)

    def pool_delete(self, ctxt, pool_id):
        self.check_mon_host(ctxt)
        pool = objects.Pool.get_by_id(
            ctxt, pool_id, expected_attrs=['crush_rule', 'osds'])
        self._check_pool_status(ctxt, pool)
        if pool['role'] == s_fields.PoolRole.INDEX:
            rgw_db = objects.RadosgwList.get_all(ctxt)
            if rgw_db:
                raise exception.InvalidInput(
                    _("Please remove the Object storage gateway first"))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.DELETE, before_obj=pool)
        pool.status = s_fields.PoolStatus.DELETING
        pool.save()
        objects.sysconfig.sys_config_set(ctxt, 'pool_undo', {})
        self.task_submit(self._pool_delete, ctxt, pool, begin_action)
        return pool

    def _pool_increase_disk(self, ctxt, pool, osd_db_ids, begin_action=None):
        try:
            crush = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                                expected_attrs=["osds"])
            osds = objects.OsdList.get_all(ctxt, filters={"id": osd_db_ids})
            new_osds = list(crush.osds) + list(osds)
            logger.info("new osds: %s", new_osds)
            gen = CrushContentGen.from_content(
                ctxt, crush.content, new_osds
            )
            crush.content = gen.gen_content()
            crush.save()
            logger.debug("crush content: %s", json.dumps(crush.content))
            ceph_client = CephTask(ctxt)
            ceph_client.pool_add_disk(pool, crush.content)
            msg = _("pool {} increase disk success").format(pool.display_name)
            pool.osd_num = len(new_osds)
            status = s_fields.PoolStatus.ACTIVE
            pool.status = status
            pool.save()
            self._update_osd_info(ctxt, osd_db_ids, crush.id)
            undo_data = {
                "pool": {
                    "id": pool.id,
                    "name": pool.display_name
                },
                "osds": [{
                    "id": osd.id,
                    "osd_name": osd.osd_name
                } for osd in osds]
            }
            op_status = "INCREASE_DISK_SUCCESS"
            objects.sysconfig.sys_config_set(ctxt, 'pool_undo', undo_data)
            err_msg = None
        except Exception as e:
            logger.error("increase disk error: {}".format(e))
            err_msg = str(e)
            msg = _("pool {} increase disk error").format(pool.display_name)
            status = s_fields.PoolStatus.ERROR
            pool.status = status
            pool.save()
            op_status = "INCREASE_DISK_ERROR"
        self.send_websocket(ctxt, pool, op_status, msg)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool, status=status, err_msg=err_msg)

    def pool_increase_disk(self, ctxt, id, data):
        self.check_mon_host(ctxt)
        pool = objects.Pool.get_by_id(ctxt, id)
        self._check_pool_status(ctxt, pool)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_ADD_DISK, before_obj=pool)
        osds = data.get('osds')
        pool.status = s_fields.PoolStatus.PROCESSING
        pool.save()
        self.task_submit(self._pool_increase_disk, ctxt, pool, osds,
                         begin_action)
        return pool

    def _pool_decrease_disk(self, ctxt, pool, osd_db_ids):
        crush = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id,
                                            expected_attrs=["osds"])
        osds = objects.OsdList.get_all(ctxt, filters={"id": osd_db_ids})
        new_osds = [osd for osd in crush.osds if osd not in osds]
        logger.info("new osds: %s", new_osds)

        gen = CrushContentGen.from_content(
            ctxt,
            content=crush.content,
            osds=new_osds,
        )
        crush.content = gen.gen_content()
        crush.save()
        logger.debug("crush content: %s", json.dumps(crush.content))
        ceph_client = CephTask(ctxt)
        ceph_client.pool_del_disk(pool, crush.content)
        pool.status = s_fields.PoolStatus.ACTIVE
        pool.osd_num = len(new_osds)
        pool.save()
        self._update_osd_info(ctxt, osd_db_ids, None)

    def _pool_decrease_task(self, ctxt, pool, osd_db_ids, begin_action=None):
        try:
            self._pool_decrease_disk(ctxt, pool, osd_db_ids)
            msg = _("{} decrease disk success").format(pool.display_name)
            op_status = "DECREASE_DISK_SUCCESS"
            status = 'success'
            err_msg = None
        except exception.StorException as e:
            logger.error("increase disk error: {}".format(e))
            msg = _("{} decrease disk error").format(pool.display_name)
            status = s_fields.PoolStatus.ERROR
            err_msg = str(e)
            pool.status = status
            pool.save()
            op_status = "DECREASE_DISK_ERROR"
        self.send_websocket(ctxt, pool, op_status, msg)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool, status=status, err_msg=err_msg)

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
            logger.warning("can't remove osds more than pool's rep size: %s",
                           rep_size)
            raise exception.InvalidInput(
                _("can't remove osds more than pool's rep size"))
        if dec_domain >= pool_avail_domain:
            logger.warning("can't remove osds more than pool's available "
                           "domain")
            raise exception.InvalidInput(
                reason=_("can't remove osds more than pool's available domain")
            )

    def pool_decrease_disk(self, ctxt, id, data):
        self.check_mon_host(ctxt)
        pool = objects.Pool.get_by_id(
            ctxt, id, expected_attrs=['crush_rule', 'osds'])
        self._check_pool_status(ctxt, pool)
        osds = data.get('osds')
        self._check_data_lost(ctxt, pool, osds)
        objects.sysconfig.sys_config_set(ctxt, 'pool_undo', {})
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_DEL_DISK, before_obj=pool)
        pool.status = s_fields.PoolStatus.PROCESSING
        pool.save()
        self.task_submit(self._pool_decrease_task, ctxt, pool, osds,
                         begin_action)
        return pool

    def pool_update_display_name(self, ctxt, id, name):
        pool = objects.Pool.get_by_id(ctxt, id)
        if pool.display_name != name:
            self._check_pool_display_name(ctxt, name)
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.UPDATE, before_obj=pool)
        logger.info("update pool name from %s to %s", pool.display_name, name)
        pool.display_name = name
        pool.save()
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool)
        return pool

    def _pool_update_policy(self, ctxt, pool, begin_action=None):
        try:
            crush = objects.CrushRule.get_by_id(
                ctxt, pool.crush_rule_id,
                expected_attrs=["osds"])
            osds = crush.osds
            logger.info("update crush osds: %s", osds)
            gen = CrushContentGen.from_content(
                ctxt,
                content=crush.content,
                osds=osds,
            )
            gen.fault_domain = pool.failure_domain_type
            crush.content = gen.gen_content()
            crush.save()
            logger.debug("crush content: %s", json.dumps(crush.content))
            ceph_client = CephTask(ctxt)
            ceph_client.update_pool(pool)
            pools = objects.PoolList.get_all(
                ctxt, filters={"crush_rule_id": crush.id})
            rule = ceph_client.update_crush_policy(pools, crush.content)
            crush.rule_id = rule.get('rule_id')
            crush.type = pool.failure_domain_type
            crush.save()
            msg = _("{} update policy success").format(pool.display_name)
            status = s_fields.PoolStatus.ACTIVE
            op_status = "UPDATE_POLICY_SUCCESS"
            pool.status = status
            pool.save()
            err_msg = None
        except exception.StorException as e:
            logger.exception("update pool policy error: %s", e)
            msg = _("{} update policy error").format(pool.display_name)
            status = s_fields.PoolStatus.ERROR
            op_status = "UPDATE_POLICY_ERROR"
            pool.status = status
            pool.save()
            err_msg = str(e)
        self.send_websocket(ctxt, pool, op_status, msg)
        self.finish_action(begin_action, resource_id=pool.id,
                           resource_name=pool.display_name,
                           after_obj=pool, status=status, err_msg=err_msg)

    def pool_update_policy(self, ctxt, id, data):
        self.check_mon_host(ctxt)
        pool = objects.Pool.get_by_id(ctxt, id)
        self._check_pool_status(ctxt, pool)
        rep_size = data.get('replicate_size')
        fault_domain = data.get('failure_domain_type')
        if pool.failure_domain_type == "rack" and fault_domain == "host":
            logger.error("can't set fault_domain from rack to host")
            raise exception.InvalidInput(
                _("can't set fault_domain from rack to host"))
        crush = objects.CrushRule.get_by_id(ctxt, pool.crush_rule_id)
        pools = objects.PoolList.get_all(
            ctxt, filters={"crush_rule_id": crush.id})
        if len(pools) > 1 and fault_domain != crush.content['fault_domain']:
            raise exception.InvalidInput(
                _("More than one pool use this crush,"
                  " not allow change fault domain"))
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.POOL,
            action=AllActionType.POOL_UPDATE_POLICY, before_obj=pool)
        pool.failure_domain_type = fault_domain
        pool.replicate_size = rep_size
        pool.status = s_fields.PoolStatus.PROCESSING
        pool.save()
        objects.sysconfig.sys_config_set(ctxt, 'pool_undo', {})
        self.task_submit(self._pool_update_policy, ctxt, pool, begin_action)
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
        logger.error('get pool fact size error, pool_name=%s', pool_name)
        return {}

    def _generate_osd_tree(self, ctxt, osds):
        node_osds = {}
        node_ids = set([i.node_id for i in osds])
        for osd in osds:
            if osd.node_id not in node_osds:
                node_osds[osd.node_id] = []
            node_osds[osd.node_id].append(osd)

        # nodes
        nodes = objects.NodeList.get_all(ctxt, filters={"id": node_ids})
        rack_nodes = {}
        for node in nodes:
            if node.rack_id not in rack_nodes:
                rack_nodes[node.rack_id] = []
            rack_nodes[node.rack_id].append(node)
            node.osds = node_osds.get(node.id, [])

        # racks
        rack_ids = set([i.rack_id for i in nodes])
        racks = objects.RackList.get_all(ctxt, filters={"id": rack_ids})
        dc_racks = {}
        for rack in racks:
            if rack.datacenter_id not in dc_racks:
                dc_racks[rack.datacenter_id] = []
            dc_racks[rack.datacenter_id].append(rack)
            rack.nodes = rack_nodes.get(rack.id, [])

        # datacenters
        dc_ids = set([i.datacenter_id for i in racks])
        dcs = objects.DatacenterList.get_all(ctxt, filters={"id": dc_ids})
        for dc in dcs:
            dc.racks = dc_racks.get(dc.id, [])
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

    def _try_out_osds(self, ctxt, osd_db_ids):
        osds = objects.OsdList.get_all(ctxt, filters={"id": osd_db_ids})
        osd_names = [osd.osd_name for osd in osds]
        ceph_client = CephTask(ctxt)
        ceph_client.mark_osds_out(osd_names)
        for osd in osds:
            osd.status = s_fields.OsdStatus.OFFLINE
            osd.save()

    def pool_undo(self, ctxt):
        undo = objects.sysconfig.sys_config_get(ctxt, 'pool_undo')
        pool = None
        if undo:
            logger.info("pool undo: %s", undo)
            pool = objects.Pool.get_by_id(
                ctxt, undo['pool']['id'],
                expected_attrs=['crush_rule', 'osds'])
            begin_action = self.begin_action(
                ctxt, resource_type=AllResourceType.POOL,
                action=AllActionType.POOL_UNDO, before_obj=pool)
            osd_db_ids = [osd['id'] for osd in undo['osds']]
            objects.sysconfig.sys_config_set(ctxt, 'pool_undo', {})
            try_out = False
            try:
                self._pool_decrease_disk(ctxt, pool, osd_db_ids)
            except Exception as e:
                try_out = True
                logger.exception("pool decrease disk error: %s", e)
            if try_out:
                try:
                    logger.warning("pool decrease disk failed. try out osd")
                    self._try_out_osds(ctxt, osd_db_ids)
                except Exception as e:
                    logger.exception("try out osds error: %s", e)
                    pool.status = s_fields.PoolStatus.ERROR
                    pool.save()
                    raise exception.StorException(str(e))
            logger.info("pool undo finish")
            self.finish_action(begin_action, resource_id=pool.id,
                               resource_name=pool.display_name,
                               after_obj=pool, status=pool.status)
            return pool
        else:
            raise exception.InvalidInput(_("No available undo"))

    def pool_get_undo(self, ctxt):
        data = objects.sysconfig.sys_config_get(ctxt, 'pool_undo')
        if not data:
            return None
        osd_db_ids = [osd['id'] for osd in data['osds']]
        osds = objects.OsdList.get_all(ctxt, filters={'id': osd_db_ids},
                                       expected_attrs=['node', 'disk'])
        self._osds_update_size(ctxt, osds)
        res = {
            'pool': objects.Pool.get_by_id(ctxt, data['pool']['id']),
            'osds': osds
        }
        return res
