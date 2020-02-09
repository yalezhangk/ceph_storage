#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.DSM.admin import AdminService
from DSpace.utils import run_loop
from DSpace.utils.coordination import COORDINATOR


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    COORDINATOR.start()
    admin = AdminService(rpc_ip=CONF.my_ip, rpc_port=CONF.admin_port)
    admin.start()
    run_loop()
    admin.stop()


if __name__ == "__main__":
    main()
