#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from oslo_log import log as logging

from DSpace import i18n
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.DSI.api import service

logger = logging.getLogger(__name__)


def main():
    CONF(sys.argv[1:], project='dspace',
         version=version.version_string())

    logging.setup(CONF, "dspace")
    languages = i18n.get_available_languages()
    logger.info("---------------------%s", languages)
    objects.register_all()
    service()


if __name__ == "__main__":
    main()
