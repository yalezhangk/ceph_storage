#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.DSI.api import service


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    service()


if __name__ == "__main__":
    main()
