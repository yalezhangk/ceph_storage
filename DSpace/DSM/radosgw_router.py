import json
import time

from netaddr import IPAddress
from netaddr import IPNetwork
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.system import System as SystemTool

logger = logging.getLogger(__name__)


class RadosgwRouterHandler(AdminBaseHandler):

    def rgw_router_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
        services = objects.RadosgwRouterList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=['radosgws', 'router_services'])
        return services

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

        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'name': data.get('name')})
        if rgw_router:
            raise exception.InvalidInput(
                _("The Router name %s has been used") % data.get('name'))

        virtual_ip = data.get('virtual_ip')
        if not virtual_ip:
            data['virtual_router_id'] = None
            return
        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'virtual_ip': virtual_ip})
        if rgw_router:
            raise exception.InvalidInput(
                _("The virtual ip %s has been used") % virtual_ip)

        rgw_router = objects.RadosgwRouterList.get_all(
            ctxt, filters={'virtual_router_id': data.get('virtual_router_id')})
        if rgw_router:
            raise exception.InvalidInput(
                _("The virtual router id %s has been used") % virtual_ip)

        # Check if virtual ip address is in gateway_cidr
        gateway_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="gateway_cidr")
        if IPAddress(virtual_ip) not in IPNetwork(gateway_cidr):
            raise exception.InvalidInput(
                _("The virtual ip address %s is not in gateway_cidr")
                % virtual_ip)

        # Check if vip can be used
        ssh_client = SSHExecutor()
        sys_tool = SystemTool(ssh_client)
        if sys_tool.ping(virtual_ip):
            raise exception.InvalidInput(
                _("The virtual ip %s is used by another node") % virtual_ip)

    def _set_router_for_rgws(self, ctxt, rgw_list, rgw_router):
        for rgw_id in rgw_list:
            rgw = objects.Radosgw.get_by_id(ctxt, rgw_id)
            rgw.router_id = rgw_router.id
            rgw.save()

    def _wait_for_vip_ready(self, rgw_router):
        logger.debug("Waiting for virtual IP to appear: %s",
                     rgw_router.virtual_ip)
        ssh_client = SSHExecutor()
        sys_tool = SystemTool(ssh_client)
        retry_times = 0
        while True:
            if retry_times == 30:
                logger.error("dsa cann't connect in 30 seconds")
                raise exception.IPConnectError(ip=rgw_router.virtual_ip)
            if sys_tool.ping(rgw_router.virtual_ip):
                break
            logger.info("Virtual IP do not appear, will try connect"
                        "after 1 second")
            retry_times += 1
            time.sleep(1)
        logger.info("Virtual IP %s is ready", rgw_router.virtual_ip)

    def _rgw_router_create(self, ctxt, rgw_router, data, begin_action):
        try:
            self._set_router_for_rgws(ctxt, data.get("radosgws"), rgw_router)
            rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router.id,
                                                         joined_load=True)
            node_list = data.get('nodes')
            for n in node_list:
                node = objects.Node.get_by_id(ctxt, n['node_id'])
                node_task = NodeTask(ctxt, node)
                node_task.rgw_router_install(rgw_router, n['net_id'])
            if rgw_router.virtual_ip:
                self._wait_for_vip_ready(rgw_router)
            rgw_router.status = s_fields.RadosgwRouterStatus.ACTIVE
            rgw_router.save()
            logger.info("client.rgw.%s create success", rgw_router.name)
            op_status = 'CREATE_SUCCESS'
            msg = _("create success: {}").format(rgw_router.name)
            err_msg = None
        except Exception as e:
            logger.error(e)
            rgw_router.status = s_fields.RadosgwRouterStatus.ERROR
            rgw_router.save()
            logger.info("client.rgw.%s create error", rgw_router.name)
            msg = _("create error: {}").format(rgw_router.name)
            op_status = 'CREATE_ERROR'
            err_msg = str(e)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, rgw_router, op_status, msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           objects.json_encode(rgw_router), rgw_router.status,
                           err_msg=err_msg)

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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, rgw_router, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           objects.json_encode(rgw_router), status,
                           err_msg=err_msg)

    def rgw_router_delete(self, ctxt, rgw_router_id):
        rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router_id,
                                                     joined_load=True)
        logger.info("Radosgw router delete %s.", rgw_router.name)
        if rgw_router.status not in [s_fields.RadosgwRouterStatus.ACTIVE,
                                     s_fields.RadosgwRouterStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " radosgw router can be deleted"))
        begin_action = self.begin_action(ctxt, Resource.RADOSGW_ROUTER,
                                         Action.DELETE)
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
            if action == "add":
                self._add_radosgw_to_router(ctxt, radosgws, rgw_router)
            elif action == "remove":
                self._delete_radosgw_from_router(ctxt, radosgws)
            rgw_router = objects.RadosgwRouter.get_by_id(
                ctxt, rgw_router.id, joined_load=True)
            nodes = json.loads(rgw_router.nodes)
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
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, rgw_router, op_status, msg)
        logger.debug("send websocket msg: %s", msg)
        self.finish_action(begin_action, rgw_router.id, rgw_router.name,
                           objects.json_encode(rgw_router), status,
                           err_msg=err_msg)

    def rgw_router_update(self, ctxt, rgw_router_id, data):
        rgw_router = objects.RadosgwRouter.get_by_id(ctxt, rgw_router_id,
                                                     joined_load=True)
        logger.info("Radosgw router update %s with %s.",
                    rgw_router.name, data)
        if rgw_router.status not in [s_fields.RadosgwRouterStatus.ACTIVE,
                                     s_fields.RadosgwRouterStatus.ERROR]:
            raise exception.InvalidInput(_("Only available and error"
                                           " radosgw router can be updated"))
        begin_action = self.begin_action(ctxt, Resource.RADOSGW_ROUTER,
                                         Action.UPDATE)
        rgw_router.status = s_fields.RadosgwRouterStatus.UPDATING
        rgw_router.save()
        self.task_submit(self._rgw_router_update, ctxt, rgw_router, data,
                         begin_action)
        return rgw_router
