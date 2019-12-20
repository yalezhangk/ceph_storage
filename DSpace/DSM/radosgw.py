import uuid

import six
from netaddr import IPAddress
from netaddr import IPNetwork
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.node import NodeTask
from DSpace.utils import template

logger = logging.getLogger(__name__)


class RadosgwHandler(AdminBaseHandler):

    def _check_radosgw_status(self, ctxt, radosgws):
        for rgw in radosgws:
            time_now = timeutils.utcnow(with_timezone=True)
            if rgw.updated_at:
                update_time = rgw.updated_at
            else:
                update_time = rgw.created_at
            time_diff = time_now - update_time
            if (rgw.status == s_fields.RadosgwStatus.ACTIVE) and \
                    (time_diff.total_seconds() > CONF.service_max_interval):
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
        node_task = NodeTask(ctxt, node)
        node_infos = node_task.node_get_infos()

        # Check if the role of node is role_object_gateway
        if not node.role_object_gateway:
            raise exception.InvalidInput(
                _("The role of node %s is not role_object_gateway")
                % data['ip_address'])

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

        # Check if ip address is valid
        node_networks = node_infos.get('networks')
        valid_ip = False
        for net in node_networks:
            if net.get('ip_address') == data['ip_address']:
                valid_ip = True
                break
        if not valid_ip:
            raise exception.InvalidInput(
                _("The ip address %s is invalid") % data['ip_address'])

        # Check if ip address is in gateway_cidr
        gateway_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="gateway_cidr")
        if IPAddress(data['ip_address']) not in IPNetwork(gateway_cidr):
            raise exception.InvalidInput(
                _("The ip address %s is not in gateway_cidr")
                % data['ip_address'])

        # Check if port is used
        if not node_task.check_port(data['port']):
            raise exception.InvalidInput(
                _("The port %s is used") % data['port'])

    def _radosgw_config_set(self, ctxt, node, radosgw, zone):
        radosgw_configs = {
            "host": node.hostname,
            "log_file": "/var/log/ceph/ceph-rgw-{}.log".format(radosgw.name),
            "rgw_frontends": "civetweb port={}:{}".format(radosgw.ip_address,
                                                          radosgw.port),
            "rgw_zone": zone.name,
            # "rgw_zonegroup": zone.zonegroup,
            # "rgw_realm": zone.realm,
        }
        for k, v in six.iteritems(radosgw_configs):
            ceph_cfg = objects.CephConfig(
                ctxt, group="client.rgw.%s" % radosgw.name, key=k, value=v,
                value_type=s_fields.ConfigType.STRING
            )
            ceph_cfg.create()

    def _get_zone_params(self, ctxt, data):
        # TODO create zonegroup and realm
        zone_name = data.get('zone')
        if not zone_name:
            zone_name = "default"
        zones = objects.RadosgwZoneList.get_all(
            ctxt, filters={'name': zone_name})
        if not zones:
            logger.info("Create zone %s", zone_name)
            zone = objects.RadosgwZone(
                ctxt, name=zone_name, zone_id=str(uuid.uuid4()),
                zonegroup="default", realm="default",
                cluster_id=ctxt.cluster_id
            )
            zone.create()
        else:
            zone = zones[0]
        logger.debug("Zone object info: %s", zone)

        pools = objects.PoolList.get_all(ctxt, filters={"role": "gateway"})
        if not pools:
            raise exception.InvalidInput(
                _("Need to create index pool before radosgw"))
        pool = pools[0]
        tpl = template.get('radosgw_zone.json.j2')
        zone_params = tpl.render(zone_id=zone.zone_id,
                                 zone_name=zone.name,
                                 data_pool=pool.pool_name)
        return zone_params, zone

    def _radosgw_create(self, ctxt, node, radosgw, begin_action, zone_params):
        try:
            task = NodeTask(ctxt, node)
            radosgw = task.ceph_rgw_install(radosgw, zone_params)
            radosgw.status = s_fields.RadosgwStatus.ACTIVE
            radosgw.save()
            node.object_gateway_ip_address = radosgw.ip_address
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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, radosgw, op_status, msg)
        self.finish_action(begin_action, radosgw.id,
                           'client.rgw.{}'.format(radosgw.name),
                           radosgw, radosgw.status, err_msg=err_msg)

    def radosgw_create(self, ctxt, data):
        logger.info("Radosgw create with %s.", data)

        node = objects.Node.get_by_id(ctxt, data['node_id'])

        self._radosgw_create_check(ctxt, node, data)
        radosgw_name = "gateway-{}-{}-{}".format(data['ip_address'],
                                                 data['port'], node.hostname)
        begin_action = self.begin_action(ctxt, Resource.RADOSGW, Action.CREATE)

        zone_params, zone = self._get_zone_params(ctxt, data)

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
                         begin_action, zone_params)
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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, radosgw, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, radosgw.id, radosgw.name,
                           radosgw, status, err_msg=err_msg)

    def _radosgw_delete_check(self, radosgw):
        # check radosgw router on radosgw
        if radosgw.router_id:
            raise exception.InvalidInput(
                _("Must remove radosgw router before radosgw"))

    def radosgw_delete(self, ctxt, rgw_id):
        radosgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
        logger.info("Radosgw delete %s.", radosgw.name)
        if radosgw.status not in [s_fields.RadosgwStatus.ACTIVE,
                                  s_fields.RadosgwStatus.INACTIVE,
                                  s_fields.RadosgwStatus.STOPPED,
                                  s_fields.RadosgwStatus.ERROR]:
            raise exception.InvalidInput(
                _("Only available 、inactive 、stopped or error radosgw can "
                  "be deleted"))
        self._radosgw_delete_check(radosgw)
        begin_action = self.begin_action(
            ctxt, Resource.RADOSGW, Action.DELETE, radosgw)
        radosgw.status = s_fields.RadosgwStatus.DELETING
        radosgw.save()
        self.task_submit(self._radosgw_delete, ctxt, radosgw, begin_action)
        return radosgw
