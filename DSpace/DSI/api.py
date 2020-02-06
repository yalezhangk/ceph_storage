import six
import socketio
import tornado.ioloop
import tornado.web
from oslo_log import log as logging
from tornado_swagger.setup import setup_swagger

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI import auth
from DSpace.DSI import handlers
from DSpace.DSI.handlers import URLRegistry
from DSpace.service import ServiceBase

logger = logging.getLogger(__name__)


def wapper_api_route(routes):
    return [tornado.web.url(path, handler)
            for path, handler in six.iteritems(routes)]


class WebSocketHandler(object):
    def __init__(self, ioloop, sio):
        self.ioloop = ioloop
        self.sio = sio

    def send_message(self, ctxt, obj, op_type, msg, resource_type=None):
        """Send WebSocket Message"""
        message = {
            'msg': msg,
            'cluster_id': ctxt.cluster_id,
            'refresh': True,
            'payload': obj,
            'resource_type':
                resource_type if resource_type else obj.obj_name(),
            'operation_type': op_type
        }
        logger.info("websocket send message: %s", objects.Json.dumps(message))
        self.ioloop.add_callback(lambda: self.sio.emit("ASYNC_RESPONSE",
                                                       message))


class WebSocketService(ServiceBase):
    service_name = "websocket"

    def __init__(self, ioloop, sio, *args, **kwargs):
        self.handler = WebSocketHandler(ioloop, sio)
        super(WebSocketService, self).__init__(*args, **kwargs)


def service():
    logger.info("api server run on %d", CONF.api_port)
    auth.register_all()
    handlers.register_all()

    sio = socketio.AsyncServer(async_mode='tornado', cors_allowed_origins='*',
                               json=objects.Json)
    URLRegistry.register(r"/ws/")(socketio.get_tornado_handler(sio))
    routers = wapper_api_route(URLRegistry().routes())
    setup_swagger(routers)
    settings = {
        "cookie_secret": CONF.cookie_secret,
        "debug": CONF.debug,
    }
    application = tornado.web.Application(routers, **settings)
    application.listen(CONF.api_port, CONF.my_ip)
    ioloop = tornado.ioloop.IOLoop.current()
    websocket = WebSocketService(ioloop, sio, CONF.my_ip, CONF.websocket_port)
    websocket.start()
    tornado.ioloop.IOLoop.current().start()
    websocket.stop()
