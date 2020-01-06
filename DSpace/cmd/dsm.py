#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import taskflows
from DSpace import version
from DSpace.common.config import CONF
from DSpace.DSM.admin import service
from DSpace.utils.coordination import COORDINATOR


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    taskflows.register_all()
    COORDINATOR.start()
    service()


if __name__ == "__main__":
    main()
