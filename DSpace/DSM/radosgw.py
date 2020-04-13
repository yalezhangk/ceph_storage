import six
from netaddr import IPAddress
from netaddr import IPNetwork
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import context
from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSM.base import AdminBaseHandler
from DSpace.DSM.object_lifecycle import RGW_LIFECYCLE_TIME_KEY as KEY
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import ConfigKey
from DSpace.objects.fields import ObjectStoreStatus
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask

logger = logging.getLogger(__name__)


DEFAULT_REALM = "realm1"
DEFAULT_ZONEGROUP = "zonegroup1"
DEFAULT_ZONE = "zone1"

rgw_pools = ['.rgw.root',
             DEFAULT_ZONE + '.rgw.meta']

POOL_SETS = {
    "domain_root": "{}.rgw.meta:root",
    "control_pool": "{}.rgw.meta:control",
    "gc_pool": "{}.rgw.meta:log_gc",
    "lc_pool": "{}.rgw.meta:log_lc",
    "log_pool": "{}.rgw.meta:log",
    "intent_log_pool": "{}.rgw.meta:log_intent",
    "usage_log_pool": "{}.rgw.meta:log_usage",
    "reshard_pool": "{}.rgw.meta:log_reshard",
    "user_keys_pool": "{}.rgw.meta:users.keys",
    "user_email_pool": "{}.rgw.meta:users.email",
    "user_swift_pool": "{}.rgw.meta:users.swift",
    "user_uid_pool": "{}.rgw.meta:users.uid",
}
ZONE_FILE_PATH = "/etc/ceph/radosgw_zone.json"


class RadosgwMixin(AdminBaseHandler):

    def check_gateway_node(self, ctxt, node):
        # check if node is role_object_gateway
        if not node.role_object_gateway:
            raise exception.InvalidInput(
                _("The role of node %s is not role_object_gateway")
                % node.hostname)
        # check node agent
        self.check_agent_available(ctxt, node)

    def check_gateway_cidr(self, ctxt, ip):
        gateway_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="gateway_cidr")
        if IPAddress(ip) not in IPNetwork(gateway_cidr):
            raise exception.InvalidInput(
                _("The ip address %s is not in gateway_cidr") % ip)

    def _check_object_store_status(self, ctxt):
        status = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.OBJECT_STORE_INIT)
        if status in [ObjectStoreStatus.ACTIVE,
                      ObjectStoreStatus.INITIALIZING]:
            return True
        return False


class RadosgwHandler(RadosgwMixin):

    def radosgw_get(self, ctxt, radosgw_id):
        radosgw = objects.Radosgw.get_by_id(
            ctxt, radosgw_id)
        return radosgw

    def _check_radosgw_status(self, ctxt, radosgws):
        for rgw in radosgws:
            time_now = timeutils.utcnow(with_timezone=True)
            if rgw.updated_at:
                update_time = rgw.updated_at
            else:
                update_time = rgw.created_at
            time_diff = time_now - update_time
            rgw = objects.Radosgw.get_by_id(ctxt, rgw.id)
            if (rgw.status == s_fields.RadosgwStatus.ACTIVE and
                    time_diff.total_seconds() > CONF.service_max_interval):
                rgw.status = s_fields.RadosgwStatus.INACTIVE
                rgw.save()

    def radosgw_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
        radosgws = objects.RadosgwList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=['node'])
        self._check_radosgw_status(ctxt, radosgws)
        return radosgws

    def radosgw_get_count(self, ctxt, filters=None):
        return objects.RadosgwList.get_count(ctxt, filters=filters)

    def _radosgw_create_check(self, ctxt, node, data):
        logger.info("Check node %s for radosgw", node.id)
        # check node
        self.check_gateway_node(ctxt, node)
        # check mon is ready
        self.check_mon_host(ctxt)

        # Check if object store is initialized.
        object_status = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.OBJECT_STORE_INIT)
        if object_status != ObjectStoreStatus.ACTIVE:
            raise exception.Invalid(
                _("Object store has not been initialized!"))

        # Check if name is used
        rgw_db = objects.RadosgwList.get_all(
            ctxt, filters={'display_name': data.get('name')})
        if rgw_db:
            raise exception.InvalidInput(
                _("The name %s is used by another radosgw") % data['name'])

        # Check if port is used
        rgw_db = objects.RadosgwList.get_all(
            ctxt, filters={'node_id': node.id, 'port': data.get('port')})
        if rgw_db:
            raise exception.InvalidInput(
                _("The port %s is used by another radosgw") % data['port'])

        # Check if ip address is gateway ip
        if not node.object_gateway_ip_address:
            raise exception.InvalidInput(_(
                "No object gateway network on this node."
            ))
        if (IPAddress(data['ip_address']) !=
                IPAddress(node.object_gateway_ip_address)):
            raise exception.InvalidInput(
                _("The ip address %s is invalid") % data['ip_address'])

        # Check if ip address is in gateway_cidr
        self.check_gateway_cidr(ctxt, data['ip_address'])

        # Check if port is used
        node_task = NodeTask(ctxt, node)
        if not node_task.check_port(data['port']):
            raise exception.InvalidInput(
                _("The port %s is used") % data['port'])

        # check pool
        pools = objects.PoolList.get_all(
            ctxt, filters={"role": s_fields.PoolRole.INDEX})
        if not pools:
            raise exception.InvalidInput(
                _("Need to create index pool before radosgw"))
        pool = pools[0]
        if pool.status not in [s_fields.PoolStatus.ACTIVE,
                               s_fields.PoolStatus.DEGRADED,
                               s_fields.PoolStatus.WARNING,
                               s_fields.PoolStatus.RECOVERING]:
            raise exception.InvalidInput(_(
                "Index pool must be active, degraded, warning or recovering"))

    def _radosgw_config_set(self, ctxt, node, radosgw, zone):
        radosgw_configs = {
            "host": node.hostname,
            "log_file": "/var/log/ceph/ceph-rgw-{}.log".format(radosgw.name),
            "rgw_frontends": "civetweb port={}:{}".format(radosgw.ip_address,
                                                          radosgw.port),
            "rgw_zone": DEFAULT_ZONE,
            "rgw_zonegroup": DEFAULT_ZONEGROUP,
            "rgw_realm": DEFAULT_REALM,
            "rgw_enable_usage_log": "true",
        }
        for k, v in six.iteritems(radosgw_configs):
            ceph_cfg = objects.CephConfig(
                ctxt, group="client.rgw.%s" % radosgw.name, key=k, value=v,
                value_type=s_fields.ConfigType.STRING
            )
            ceph_cfg.create()

    def _get_zone_params(self, ctxt):
        zones = objects.RadosgwZoneList.get_all(
            ctxt, filters={'name': DEFAULT_ZONE})
        zone = zones[0]
        logger.debug("Zone object info: %s", zone)
        return zone

    def _radosgw_create(self, ctxt, node, radosgw, begin_action):
        try:
            task = NodeTask(ctxt, node)
            radosgw = task.ceph_rgw_install(radosgw)
            radosgw.status = s_fields.RadosgwStatus.ACTIVE
            radosgw.save()
            node.save()
            self.notify_node_update(ctxt, node)
            logger.info("client.rgw.%s create success", radosgw.name)
            op_status = 'CREATE_SUCCESS'
            msg = _("create radosgw success: {}").format(radosgw.display_name)
            err_msg = None
        except exception.StorException as e:
            logger.error(e)
            radosgw.status = s_fields.RadosgwStatus.ERROR
            radosgw.save()
            logger.info("client.rgw.%s create error", radosgw.name)
            msg = _("create error: {}").format(radosgw.display_name)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)
        self.send_websocket(ctxt, radosgw, op_status, msg)
        self.finish_action(begin_action, radosgw.id,
                           radosgw.display_name,
                           radosgw, radosgw.status, err_msg=err_msg)

    def radosgw_create(self, ctxt, data):
        logger.info("Radosgw create with %s.", data)

        node = objects.Node.get_by_id(ctxt, data['node_id'])

        self._radosgw_create_check(ctxt, node, data)
        radosgw_name = "gateway-{}-{}-{}".format(data['ip_address'],
                                                 data['port'], node.hostname)
        begin_action = self.begin_action(ctxt, Resource.RADOSGW, Action.CREATE)

        zone = self._get_zone_params(ctxt)

        radosgw = objects.Radosgw(
            ctxt, name=radosgw_name,
            description=data.get('description'),
            display_name=data.get('name'),
            status=s_fields.RadosgwStatus.CREATING,
            ip_address=data.get('ip_address'),
            port=data.get('port'),
            node_id=node.id,
            cluster_id=ctxt.cluster_id,
            zone_id=zone.id
        )
        radosgw.create()

        self._radosgw_config_set(ctxt, node, radosgw, zone)

        # apply async
        self.task_submit(self._radosgw_create, ctxt, node, radosgw,
                         begin_action)
        logger.debug("Radosgw create task apply.")
        return radosgw

    def _radosgw_config_remove(self, ctxt, radosgw):
        logger.debug("radosgw %s clear configs in db", radosgw.name)
        radosgw_cfgs = objects.CephConfigList.get_all(
            ctxt, filters={'group': "client.rgw.%s" % radosgw.name}
        )
        for cfg in radosgw_cfgs:
            cfg.destroy()

    def _radosgw_delete(self, ctxt, radosgw, begin_action):
        logger.info("trying to delete radosgw %s", radosgw.name)
        try:
            node = objects.Node.get_by_id(ctxt, radosgw.node_id)
            task = NodeTask(ctxt, node)

            radosgw = task.ceph_rgw_uninstall(radosgw)
            self._radosgw_config_remove(ctxt, radosgw)
            radosgw.destroy()
            self.notify_node_update(ctxt, node)
            msg = _("delete radosgw {} success").format(radosgw.display_name)
            logger.info("delete %s success", radosgw.name)
            status = 'success'
            op_status = "DELETE_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            logger.error("delete %s error: %s", radosgw.name, e)
            status = s_fields.RadosgwStatus.ERROR
            radosgw.status = status
            radosgw.save()
            err_msg = str(e)
            msg = _("delete radosgw {} error").format(radosgw.display_name)
            op_status = "DELETE_ERROR"
        logger.info("radosgw_delete, got radosgw: %s, radosgw name: %s",
                    radosgw, radosgw.name)
        self.send_websocket(ctxt, radosgw, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, radosgw.id, radosgw.display_name,
                           radosgw, status, err_msg=err_msg)

    def _radosgw_delete_check(self, ctxt, radosgw):
        # radosgw common check
        node = objects.Node.get_by_id(ctxt, radosgw.node_id)
        self.check_agent_available(ctxt, node)

        # check radosgw status
        if radosgw.status not in [s_fields.RadosgwStatus.ACTIVE,
                                  s_fields.RadosgwStatus.INACTIVE,
                                  s_fields.RadosgwStatus.STOPPED,
                                  s_fields.RadosgwStatus.ERROR]:
            raise exception.InvalidInput(
                _("Only available 、inactive 、stopped or error radosgw can "
                  "be deleted"))

        # check radosgw router on radosgw
        if radosgw.router_id:
            raise exception.InvalidInput(
                _("Must remove radosgw router before radosgw"))

    def radosgw_delete(self, ctxt, rgw_id):
        radosgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
        logger.info("Radosgw delete %s.", radosgw.name)
        self._radosgw_delete_check(ctxt, radosgw)
        begin_action = self.begin_action(
            ctxt, Resource.RADOSGW, Action.DELETE, radosgw)
        radosgw.status = s_fields.RadosgwStatus.DELETING
        radosgw.save()
        self.task_submit(self._radosgw_delete, ctxt, radosgw, begin_action)
        return radosgw

    def object_store_init_get(self, ctxt):
        status = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.OBJECT_STORE_INIT)
        if not status:
            status = "inactive"
        return status

    def _create_meta_pools(self, ctxt, index_pool, ceph_task):
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

        crush_rule = objects.CrushRule.get_by_id(
            ctxt, index_pool.crush_rule_id)
        # Remove object meta pools if exists
        pools = objects.PoolList.get_all(
            ctxt, filters={"role": s_fields.PoolRole.OBJECT_META})
        for p in pools:
            ceph_task.pool_delete(p)
            p.destroy()
        # Create object meta pools
        for pool_name in rgw_pools:
            pool = objects.Pool(
                ctxt,
                cluster_id=ctxt.cluster_id,
                status=s_fields.PoolStatus.CREATING,
                pool_name=pool_name,
                display_name=pool_name,
                type=index_pool.type,
                role=s_fields.PoolRole.OBJECT_META,
                data_chunk_num=index_pool.data_chunk_num,
                coding_chunk_num=index_pool.coding_chunk_num,
                osd_num=index_pool.osd_num,
                speed_type=index_pool.speed_type,
                replicate_size=index_pool.replicate_size,
                failure_domain_type=index_pool.failure_domain_type)
            pool.create()
            ceph_task.pool_create(pool, specified_rep,
                                  crush_rule.content)
            pool.status = s_fields.PoolStatus.ACTIVE
            pool.save()

    def _initialize_zone(self, ctxt, client):
        zone = client.rgw_zone_init(ctxt, DEFAULT_REALM, DEFAULT_ZONEGROUP,
                                    DEFAULT_ZONE, ZONE_FILE_PATH, POOL_SETS)
        zones = objects.RadosgwZoneList.get_all(ctxt)
        for z in zones:
            z.destroy()
        zone_db = objects.RadosgwZone(
            ctxt, name=DEFAULT_ZONE, zone_id=zone["id"],
            zonegroup=DEFAULT_ZONEGROUP, realm=DEFAULT_REALM,
            cluster_id=ctxt.cluster_id
        )
        zone_db.create()
        client.placement_remove(ctxt, "default-placement")
        client.period_update(ctxt=ctxt)

    def _system_user_create(self, ctxt, client):
        user = client.user_create_cmd(
            ctxt, "t2stor", display_name="T2stor User", access_key="portal",
            secret_key="portal")
        user = client.caps_add(ctxt, user["user_id"],
                               "users=*;usage=*;buckets=*;metadata=*")
        client.period_update(ctxt=ctxt)
        admin_user = {
            "uid": user['user_id'],
            "display_name": user['display_name'],
            "email": user['email'],
            "suspended": user['suspended'],
            "status": "active",
            "max_buckets": user['max_buckets'],
            "op_mask": user['op_mask'],
            "bucket_quota_max_size": user['bucket_quota']['max_size'],
            "bucket_quota_max_objects": user['bucket_quota']['max_objects'],
            "user_quota_max_size": user['user_quota']['max_size'],
            "user_quota_max_objects": user['user_quota']['max_objects'],
            "is_admin": 1,
            "cluster_id": ctxt.cluster_id,
            "capabilities": "users=*;usage=*;buckets=*;metadata=*"
        }
        object_user = objects.ObjectUser(ctxt, **admin_user)
        object_user.create()
        user_access = {
            "obj_user_id": object_user.id,
            "access_key": user['keys'][0]['access_key'],
            "secret_key": user['keys'][0]['secret_key'],
            "type": "s3",
            "cluster_id": ctxt.cluster_id
        }
        object_access_key = objects.ObjectAccessKey(ctxt, **user_access)
        object_access_key.create()

    def _init_lifecycle_work_time(self, ctxt):
        value, value_type = self.object_lifecycle_get_default_work_time(ctxt)
        cephconf = objects.CephConfig(
            ctxt, group='global', key=KEY, value=value,
            value_type=value_type, cluster_id=ctxt.cluster_id)
        cephconf.create()
        logger.info('lifecycle default work time: %s set success in db', value)

    def _object_store_init(self, ctxt, index_pool, begin_action):
        ceph_task = CephTask(ctxt)
        try:
            # Create meta pools
            self._create_meta_pools(ctxt, index_pool, ceph_task)
            # Set zonegroup、zone and t2stor user
            nodes = objects.NodeList.get_all(ctxt,
                                             filters={"role_monitor": True})
            mon_node = nodes[0]
            client = context.agent_manager.get_client(node_id=mon_node.id)
            self._initialize_zone(ctxt, client)
            self._system_user_create(ctxt, client)
            self._init_lifecycle_work_time(ctxt)
            status = s_fields.ObjectStoreStatus.ACTIVE
            msg = _("Initialize object store successfully.")
            op_status = "INIT_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            logger.exception("create pool error: %s", e)
            err_msg = str(e)
            status = s_fields.ObjectStoreStatus.ERROR
            msg = _("Initialize object store failed!")
            op_status = "INIT_ERROR"

        objects.sysconfig.sys_config_set(
            ctxt, ConfigKey.OBJECT_STORE_INIT, status, "string")
        self.send_websocket(ctxt, {}, op_status, msg,
                            resource_type="ObjectStore")
        self.finish_action(
            begin_action, resource_id=index_pool.id,
            resource_name=index_pool.display_name,
            after_obj=index_pool, status=status, err_msg=err_msg)

    def object_store_init_check(self, ctxt, index_pool):
        # check mon is ready
        self.check_mon_host(ctxt)
        # check object store init status
        if self._check_object_store_status(ctxt):
            raise exception.Invalid(_("Object store has been initialized!"))
        # check pool status
        if index_pool.status not in [s_fields.PoolStatus.ACTIVE,
                                     s_fields.PoolStatus.WARNING]:
            raise exception.Invalid(_("Pool status must be active or warning"))
        # check role of the pool
        if index_pool.role != s_fields.PoolRole.INDEX:
            raise exception.Invalid(
                _("Pool %s is not index pool!") % index_pool.display_name)

    def object_store_init(self, ctxt, index_pool_id):
        logger.info("Init radosgw for cluster %s", ctxt.cluster_id)
        index_pool = objects.Pool.get_by_id(ctxt, index_pool_id)
        self.object_store_init_check(ctxt, index_pool)
        begin_action = self.begin_action(
            ctxt, resource_type=s_fields.AllResourceType.OBJECT_STORE,
            action=s_fields.AllActionType.OBJECT_STORE_INITIALIZE)

        objects.sysconfig.sys_config_set(
            ctxt, ConfigKey.OBJECT_STORE_INIT, ObjectStoreStatus.INITIALIZING,
            "string")
        objects.sysconfig.sys_config_set(ctxt, ConfigKey.OBJECT_META_POOL,
                                         index_pool_id, "int")
        self.task_submit(self._object_store_init, ctxt, index_pool,
                         begin_action)
        return ObjectStoreStatus.INITIALIZING
