import socketio
import tornado.ioloop
import tornado.web
from oslo_log import log as logging
from tornado_swagger.setup import setup_swagger

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.handlers import get_routers
from DSpace.service import ServiceBase

logger = logging.getLogger(__name__)


def wapper_api_route(routes):
    api_prefix = CONF.api_prefix
    return [tornado.web.url(api_prefix + path, handler)
            for path, handler in routes]


class WebSocketHandler(object):
    def __init__(self, ioloop, sio):
        self.ioloop = ioloop
        self.sio = sio

    def send_message(self, ctxt, obj, op_type, msg):
        """Send WebSocket Message"""
        message = {
            'msg': msg,
            'cluster_id': ctxt.cluster_id,
            'refresh': True,
            'payload': obj,
            'resource_type': obj.obj_name() if obj else None,
            'operation_type': op_type
        }
        self.ioloop.add_callback(lambda: self.sio.emit("ASYNC_RESPONSE",
                                                       message))


class WebSocketService(ServiceBase):
    service_name = "websocket"
    rpc_endpoint = None
    rpc_ip = "192.168.211.129"
    rpc_port = 2081

    def __init__(self, ioloop, sio):
        self.handler = WebSocketHandler(ioloop, sio)
        self.rpc_endpoint = {
            "ip": self.rpc_ip,
            "port": self.rpc_port
        }
        super(WebSocketService, self).__init__()


def service():
    logger.info("api server run on %d", CONF.api_port)
    sio = socketio.AsyncServer(async_mode='tornado', cors_allowed_origins='*',
                               json=objects.Json)
    routers = get_routers()
    routers += [(r"/ws/", socketio.get_tornado_handler(sio))]
    routers = wapper_api_route(routers)
    setup_swagger(routers)
    settings = {
        "cookie_secret": CONF.cookie_secret,
        "debug": CONF.debug,
    }
    application = tornado.web.Application(routers, **settings)
    application.listen(CONF.api_port, CONF.my_ip)
    ioloop = tornado.ioloop.IOLoop.current()
    websocket = WebSocketService(ioloop, sio)
    websocket.start()
    tornado.ioloop.IOLoop.current().start()
    websocket.stop()
