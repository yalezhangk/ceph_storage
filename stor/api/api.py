import sys
import json

import tornado.ioloop
import tornado.web
from tornado.websocket import WebSocketHandler
from oslo_log import log as logging

from stor import objects
from stor import version
from stor.common.config import CONF
from stor.api.handlers import get_routers
from stor.service import ServiceBase


logger = logging.getLogger(__name__)


class EchoWebSocket(WebSocketHandler):
    clients = {}

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


def main():
    objects.register_all()
    routers = get_routers()
    routers += [(r"/ws", EchoWebSocket)]
    application = tornado.web.Application(routers, debug=CONF.debug)
    logger.info("server run on xxxx")
    application.listen(8888)
    ioloop = tornado.ioloop.IOLoop.current()
    WebSocketService(ioloop).start()
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    main()
