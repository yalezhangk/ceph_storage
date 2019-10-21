#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from oslo_log import log as logging

from t2stor import objects
from t2stor import version
from t2stor.admin.admin import service
from t2stor.common.config import CONF


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    service()


if __name__ == "__main__":
    main()
