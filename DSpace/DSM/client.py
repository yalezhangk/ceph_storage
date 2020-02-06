from __future__ import print_function

import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClientManager
from DSpace.service import RPCClient


class AdminClientManager(BaseClientManager):
    cluster = "default"
    service_name = "admin"
    client_cls = RPCClient


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False,
                          request_id='xxxxxxxxxxxxxxxxxxxxxxxxx')
    logger = logging.getLogger(__name__)
    logger.info('Simple')
