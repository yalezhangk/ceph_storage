import sys

import tornado.ioloop
import tornado.web
from oslo_log import log as logging

from ..scheduler import SchedulerClientManager
from ..agent import AgentClientManager
from stor import objects
from stor import version
from stor.common.config import CONF
from stor.api.handlers import get_routers

logger = logging.getLogger(__name__)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


class CephConfHandler(tornado.web.RequestHandler):
    def get(self):
        location = 'example'
        scheduler = SchedulerClientManager()
        res = scheduler.get_client().get_ceph_conf(location=location)
        self.write(res)


class HostDisksHandler(tornado.web.RequestHandler):
    def get(self):
        host = self.get_argument("host")
        if not host:
            self.write("Please add host!")
        agent = AgentClientManager()
        disks = agent.get_client("whx-ceph-1").get_host_disks(host)
        self.write(disks)


class AppendCephMonitorHandler(tornado.web.RequestHandler):
    def get(self):
        location = 'example'
        scheduler = SchedulerClientManager()
        res = scheduler.get_client().AppendCephMonitor(location=location)
        self.write(res)


def main():
    objects.register_all()
    routers = [
        (r"/", MainHandler),
        (r"/cephconf", CephConfHandler),
        (r"/host/disks", HostDisksHandler),
        (r"/host/appendmon", AppendCephMonitorHandler),
    ]
    routers = routers + get_routers()
    application = tornado.web.Application(routers)
    logger.info("server run on xxxx")
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "cinder")
    main()
