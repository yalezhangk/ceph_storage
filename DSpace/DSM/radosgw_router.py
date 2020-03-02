import json
import time

from netaddr import IPAddress
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSM.radosgw import RadosgwMixin
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.system import System as SystemTool

logger = logging.getLogger(__name__)


class RadosgwRouterHandler(RadosgwMixin):

    def _check_router_status(self, routers):
        for router in routers:
            router_health = True
            for service in router.router_services:
                time_now = timeutils.utcnow(with_timezone=True)
                if service.updated_at:
                    update_time = service.updated_at
                else:
                    update_time = service.created_at
                time_diff = time_now - update_time
                if time_diff.total_seconds() > CONF.service_max_interval:
                    service.status = s_fields.ServiceStatus.INACTIVE
                    service.save()
                if service.status == s_fields.ServiceStatus.INACTIVE:
                    router_health = False
            if router.status in [s_fields.RadosgwRouterStatus.CREATING,
                                 s_fields.RadosgwRouterStatus.DELETING,
                                 s_fields.RadosgwRouterStatus.UPDATING,
                                 s_fields.RadosgwRouterStatus.ERROR]:
                continue
            if router_health:
                router.status = s_fields.RadosgwRouterStatus.ACTIVE
            else:
                router.status = s_fields.RadosgwRouterStatus.INACTIVE
            router.save()

    def rgw_router_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        routers = objects.RadosgwRouterList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=['radosgws', 'router_services'])
        self._check_router_status(routers)
        return routers

    def rgw_router_get_count(self, ctxt, filters=None):
        return objects.RadosgwRouterList.get_count(ctxt, filters=filters)

    def _rgw_router_create_check(self, ctxt, data):
        logger.info("Check create for rgw router")

        # check radosgw has been routed
        for rgw_id in data.get("radosgws"):
            rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
            if rgw.router_id:
                raise exception.InvalidInput(
                    _("The radosgw %s has router") % rgw.display_name)

        # check if router name is used
        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'name': data.get('name')})
        if rgw_router:
            raise exception.InvalidInput(
                _("The Router name %s has been used") % data.get('name'))

        # check if vip is used
        virtual_ip = data.get('virtual_ip')
        if not virtual_ip:
            data['virtual_router_id'] = None
            return
        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'virtual_ip': virtual_ip})
        if rgw_router:
            raise exception.InvalidInput(
                _("The virtual ip %s has been used") % virtual_ip)

        # check if virtual id is used
        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'virtual_router_id': data.get('virtual_router_id')})
        if rgw_router:
            raise exception.InvalidInput(
                _("The virtual router id %s has been used")
                % data.get('virtual_router_id'))

        # Check if virtual ip address is in gateway_cidr
        self.check_gateway_cidr(ctxt, virtual_ip)

        # Check http and https port
        http_port = data.get('port')
        https_port = data.get('https_port')
        if http_port == https_port:
            raise exception.InvalidInput(
                _("HTTP port and HTTPS port can not be same"))

        # check node
        nodes = data.get("nodes")
        node0 = None
        for n in nodes:
            node = objects.Node.get_by_id(ctxt, n.get("node_id"))
            if not node0:
                node0 = node
            # check if node is role_object_gateway
            self.check_gateway_node(ctxt, node)
            # check if node is used by another router
            service = objects.RouterServiceList.get_all(
                ctxt, filters={"node_id": node.id}
            )
            if service:
                node = objects.Node.get_by_id(ctxt, n.get("node_id"))
                raise exception.InvalidInput(
                    _("The node %s has radosgw router") % node.hostname)

        # Check if vip is used by another node
        ssh_client = SSHExecutor(hostname=str(node0.ip_address),
                                 password=node0.password)
        sys_tool = SystemTool(ssh_client)
        if sys_tool.ping(virtual_ip):
            raise exception.InvalidInput(
                _("The virtual ip %s is used by another node") % virtual_ip)

    def _set_router_for_rgws(self, ctxt, rgw_list, rgw_router):
        for rgw_id in rgw_list:
            rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
            rgw.router_id = rgw_router.id
            rgw.save()

    def _wait_for_vip_ready(self, rgw_router, node0):
        logger.debug("Waiting for virtual IP to appear: %s",
                     rgw_router.virtual_ip)
        ssh_client = SSHExecutor(hostname=str(node0.ip_address),
                                 password=node0.password)
        sys_tool = SystemTool(ssh_client)
        retry_times = 0
        while True:
            if retry_times == 30:
                logger.error("VIP cann't connect in 30 seconds")
                raise exception.IPConnectError(ip=rgw_router.virtual_ip)
            if sys_tool.ping(rgw_router.virtual_ip):
                break
            logger.info("Virtual IP do not appear, will try connect"
                        "after 1 second, count=%s", retry_times)
            retry_times += 1
            time.sleep(1)
        logger.info("Virtual IP %s is ready", rgw_router.virtual_ip)

    def _rgw_router_create(self, ctxt, rgw_router, data, begin_action):
        try:
            self._set_router_for_rgws(ctxt, data.get("radosgws"), rgw_router)
            rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router.id,
                                                         joined_load=True)
            node_list = data.get('nodes')
            node0 = None
            for n in node_list:
                node = objects.Node.get_by_id(ctxt, n['node_id'])
                if not node0:
                    node0 = node
                node_task = NodeTask(ctxt, node)
                node_task.rgw_router_install(rgw_router, n['net_id'])
                self.notify_node_update(ctxt, node)
            if rgw_router.virtual_ip:
                self._wait_for_vip_ready(rgw_router, node0)
            rgw_router.status = s_fields.RadosgwRouterStatus.ACTIVE
            rgw_router.save()
            logger.info("client.rgw.%s create success", rgw_router.name)
            op_status = 'CREATE_SUCCESS'
            msg = _("create radosgw success: {}").format(rgw_router.name)
            err_msg = None
        except Exception as e:
            logger.error(e)
            rgw_router.status = s_fields.RadosgwRouterStatus.ERROR
            rgw_router.save()
            logger.info("client.rgw.%s create error", rgw_router.name)
            msg = _("create error: {}").format(rgw_router.name)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)
        self.send_websocket(ctxt, rgw_router, op_status, msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           rgw_router, rgw_router.status, err_msg=err_msg)

    def rgw_router_create(self, ctxt, data):
        logger.info("Radosgw router create with %s.", data)
        self._rgw_router_create_check(ctxt, data)

        begin_action = self.begin_action(ctxt, Resource.RADOSGW_ROUTER,
                                         Action.CREATE)
        virtual_ip = data.get('virtual_ip')
        rgw_router = objects.RadosgwRouter(
            ctxt, name=data.get('name'),
            description=data.get('description'),
            status=s_fields.RadosgwRouterStatus.CREATING,
            virtual_ip=IPAddress(virtual_ip) if virtual_ip else None,
            virtual_router_id=data.get('virtual_router_id'),
            port=data.get('port'),
            https_port=data.get('https_port', 443),
            nodes=json.dumps(data.get('nodes')),
            cluster_id=ctxt.cluster_id,
        )
        rgw_router.create()
        # apply async
        self.task_submit(self._rgw_router_create, ctxt, rgw_router, data,
                         begin_action)
        logger.debug("Radosgw router create task apply.")
        return rgw_router

    def _delete_router_services(self, rgw_router):
        radosgws = rgw_router.radosgws
        for rgw in radosgws:
            rgw.router_id = None
            rgw.save()
        services = rgw_router.router_services
        for service in services:
            service.destroy()

    def _rgw_router_delete(self, ctxt, rgw_router, begin_action):
        logger.info("trying to delete rgw_router %s", rgw_router.name)
        try:
            self._delete_router_services(rgw_router)
            nodes = json.loads(rgw_router.nodes)
            for n in nodes:
                node = objects.Node.get_by_id(ctxt, n['node_id'])
                task = NodeTask(ctxt, node)
                task.rgw_router_uninstall(rgw_router)
                self.notify_node_update(ctxt, node)
            rgw_router.destroy()
            msg = _("delete radosgw router {} success").format(rgw_router.name)
            logger.info("delete %s success", rgw_router.name)
            status = 'success'
            op_status = "DELETE_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            logger.error("delete %s error: %s", rgw_router.name, e)
            status = s_fields.RadosgwRouterStatus.ERROR
            rgw_router.status = status
            rgw_router.save()
            err_msg = str(e)
            msg = _("delete rgw_router {} error").format(rgw_router.name)
            op_status = "DELETE_ERROR"
        logger.info("radosgw delete, got radosgw router: %s, name: %s",
                    rgw_router, rgw_router.name)
        self.send_websocket(ctxt, rgw_router, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           rgw_router, status, err_msg=err_msg)

    def _router_node_check(self, ctxt, rgw_router):
        nodes = json.loads(rgw_router.nodes)
        for n in nodes:
            node = objects.Node.get_by_id(ctxt, n['node_id'])
            self.check_agent_available(ctxt, node)

    def _rgw_router_delete_check(self, ctxt, rgw_router):
        # check router status
        if rgw_router.status not in [s_fields.RadosgwRouterStatus.ACTIVE,
                                     s_fields.RadosgwRouterStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " radosgw router can be deleted"))
        # check router node
        self._router_node_check(ctxt, rgw_router)

    def rgw_router_delete(self, ctxt, rgw_router_id):
        rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router_id,
                                                     joined_load=True)
        logger.info("Radosgw router delete %s.", rgw_router.name)
        self._rgw_router_delete_check(ctxt, rgw_router)
        begin_action = self.begin_action(ctxt, Resource.RADOSGW_ROUTER,
                                         Action.DELETE, rgw_router)
        rgw_router.status = s_fields.RadosgwRouterStatus.DELETING
        rgw_router.save()
        self.task_submit(self._rgw_router_delete, ctxt, rgw_router,
                         begin_action)
        return rgw_router

    def _delete_radosgw_from_router(self, ctxt, radosgws):
        for rgw_id in radosgws:
            rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
            rgw.router_id = None
            rgw.save()

    def _add_radosgw_to_router(self, ctxt, radosgws, rgw_router):
        for rgw_id in radosgws:
            rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
            rgw.router_id = rgw_router.id
            rgw.save()

    def _rgw_router_update(self, ctxt, rgw_router, data, begin_action):
        logger.info("trying to update rgw_router %s", rgw_router.name)
        action = data.get('action')
        radosgws = data.get('radosgws')
        try:
            rgw_router = objects.RadosgwRouter.get_by_id(
                ctxt, rgw_router.id, joined_load=True)
            nodes = json.loads(rgw_router.nodes)
            if action == "add":
                self._add_radosgw_to_router(ctxt, radosgws, rgw_router)
            elif action == "remove":
                self._delete_radosgw_from_router(ctxt, radosgws)
            for n in nodes:
                node = objects.Node.get_by_id(ctxt, n['node_id'])
                task = NodeTask(ctxt, node)
                task.rgw_router_update()
            rgw_router.status = s_fields.RadosgwRouterStatus.ACTIVE
            rgw_router.save()
            msg = _("Update radosgw router {} success").format(rgw_router.name)
            logger.info("Update %s success", rgw_router.name)
            status = 'success'
            op_status = "UPDATE_SUCCESS"
            err_msg = None
        except exception.StorException as e:
            logger.error("delete %s error: %s", rgw_router.name, e)
            status = s_fields.RadosgwRouterStatus.ERROR
            rgw_router.status = status
            rgw_router.save()
            err_msg = str(e)
            msg = _("Update rgw_router {} error").format(rgw_router.name)
            op_status = "UPDATE_ERROR"
        logger.info("radosgw update, got radosgw router: %s, name: %s",
                    rgw_router, rgw_router.name)
        self.send_websocket(ctxt, rgw_router, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           rgw_router, status, err_msg=err_msg)

    def _rgw_router_update_check(self, ctxt, rgw_router, data):
        # router status check
        if rgw_router.status not in [s_fields.RadosgwRouterStatus.ACTIVE,
                                     s_fields.RadosgwRouterStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " radosgw router can be updated"))
        # node check
        self._router_node_check(ctxt, rgw_router)
        # action check
        action = data.get('action')
        if action == "remove":
            # check if rgw is the last
            rgws = objects.RadosgwList.get_all(
                ctxt, filters={"router_id": rgw_router.id})
            if len(rgws) == len(data.get("radosgws")):
                raise exception.InvalidInput(
                    _("There must be at least one rgw in router"))

    def rgw_router_update(self, ctxt, rgw_router_id, data):
        rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router_id,
                                                     joined_load=True)
        logger.info("Radosgw router update %s with %s.",
                    rgw_router.name, data)
        self._rgw_router_update_check(ctxt, rgw_router, data)

        action = data.get('action')
        if action == "add":
            action_type = Action.RGW_ROUTER_ADD
        if action == "remove":
            action_type = Action.RGW_ROUTER_REMOVE
        begin_action = self.begin_action(ctxt, Resource.RADOSGW_ROUTER,
                                         action_type, rgw_router)
        rgw_router.status = s_fields.RadosgwRouterStatus.UPDATING
        rgw_router.save()
        self.task_submit(self._rgw_router_update, ctxt, rgw_router, data,
                         begin_action)
        return rgw_router
