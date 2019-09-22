import sys

import tornado.ioloop
import tornado.web
from oslo_log import log as logging

from stor import objects
from stor import version
from stor.common.config import CONF
from stor.api.handlers import get_routers

logger = logging.getLogger(__name__)


def main():
    objects.register_all()
    routers = get_routers()
    application = tornado.web.Application(routers)
    logger.info("server run on xxxx")
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    main()
