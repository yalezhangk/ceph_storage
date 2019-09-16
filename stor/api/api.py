import logging

import tornado.ioloop
import tornado.web

from ..scheduler import SchedulerClientManager
from ..agent import AgentClientManager

logger = logging.getLogger(__name__)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class CephConfHandler(tornado.web.RequestHandler):
    def get(self):
        location='example'
        schduler  = SchedulerClientManager()
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
        location='example'
        schduler  = SchedulerClientManager()
        res = schduler.get_client().AppendCephMonitor(location=location)
        self.write(res)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    logger.info("server run on xxxx")
    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/cephconf", CephConfHandler),
        (r"/host/disks", HostDisksHandler),
        (r"/host/appendmon", AppendCephMonitorHandler),
    ])
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()
