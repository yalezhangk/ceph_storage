import json

import tornado.ioloop
import tornado.web
from oslo_log import log as logging
from tornado.websocket import WebSocketHandler

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.handlers import get_routers
from DSpace.service import ServiceBase

logger = logging.getLogger(__name__)


class EchoWebSocket(WebSocketHandler):
    clients = {}

    def check_origin(self, origin):
        return True

    def open(self):
        logger.debug("WebSocket opened")
        self.clients['user_id'] = self
        self.write_message(u"3probe")

    def on_message(self, message):
        logger.debug("WebSocket(%s) on message: %s" % (self, message))
        if message == "2":
            self.write_message(u"3")
        else:
            self.write_message(u"You said: " + message)

    def on_close(self):
        logger.debug("WebSocket closed")
        self.clients.pop("user_id", None)

    def on_pong(self, data):
        logger.debug("WebSocket(%s) on pong: %s" % (self, data))

    def on_ping(self, data):
        logger.debug("WebSocket(%s) on ping: %s" % (self, data))

    @classmethod
    def notify(cls, context, obj, op_type, msg=None):
        client = cls.clients.get(context.user_id)
        if not client:
            return
        message = {
            'msg': msg,
            'cluster_id': context.cluster_id,
            'refresh': True,
            'payload': obj,
            'resource_type': obj.obj_name() if obj else None,
            'operation_type': op_type
        }
        client.write_message(objects.json_encode(message))


class WebSocketHandler(object):
    def __init__(self, ioloop, *args, **kwargs):
        self.ioloop = ioloop
        super(WebSocketHandler, self).__init__(*args, **kwargs)

    def send_message(self, ctxt, obj, op_type, msg):
        """Send WebSocket Message"""
        self.ioloop.add_callback(
            lambda: EchoWebSocket.notify(ctxt, obj, op_type, msg)
        )


class WebSocketService(ServiceBase):
    service_name = "websocket"
    rpc_endpoint = None
    rpc_ip = "192.168.211.129"
    rpc_port = 2081

    def __init__(self, ioloop):
        self.handler = WebSocketHandler(ioloop)
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })
        super(WebSocketService, self).__init__()


def service():
    logger.info("api server run on %d", CONF.api_port)
    routers = get_routers()
    routers += [(r"/ws/", EchoWebSocket)]
    settings = {
        "cookie_secret": CONF.cookie_secret,
        "debug": CONF.debug,
    }
    application = tornado.web.Application(routers, **settings)
    application.listen(CONF.api_port, CONF.my_ip)
    ioloop = tornado.ioloop.IOLoop.current()
    websocket = WebSocketService(ioloop)
    websocket.start()
    tornado.ioloop.IOLoop.current().start()
    websocket.stop()
