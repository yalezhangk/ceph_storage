import json

import tornado.ioloop
import tornado.web
from oslo_log import log as logging
from tornado.websocket import WebSocketHandler

from t2stor.api.handlers import get_routers
from t2stor.common.config import CONF
from t2stor.service import ServiceBase

logger = logging.getLogger(__name__)


class EchoWebSocket(WebSocketHandler):
    clients = {}

    def check_origin(self, origin):
        return CONF.check_origin

    def open(self):
        logger.debug("WebSocket opened")
        self.clients['user_id'] = self

    def on_message(self, message):
        logger.debug("WebSocket(%s) on message: %s" % (self, message))
        self.write_message(u"You said: " + message)

    def on_close(self):
        logger.debug("WebSocket closed")
        self.clients.pop("user_id")


class WebSocketHandler(object):
    def __init__(self, ioloop, *args, **kwargs):
        self.ioloop = ioloop
        super(WebSocketHandler, self).__init__(*args, **kwargs)

    def send_message(self, ctxt, message):
        """Send WebSocket Message"""
        self.ioloop.add_callback(
            lambda: EchoWebSocket.clients["user"].write_message(message)
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
    routers += [(r"/ws", EchoWebSocket)]
    application = tornado.web.Application(routers, debug=CONF.debug)
    application.listen(CONF.api_port, CONF.my_ip)
    ioloop = tornado.ioloop.IOLoop.current()
    websocket = WebSocketService(ioloop)
    websocket.start()
    tornado.ioloop.IOLoop.current().start()
    websocket.stop()
