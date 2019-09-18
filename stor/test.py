#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import logging
from oslo_config import cfg
from stor.objects.volume import Volume
from stor.objects.volume import VolumeList
from stor.common.config import CONF
from stor.context import RequestContext

LOG = logging.getLogger(__name__)

cfg.CONF(sys.argv[1:])
CONF.log_opt_values(LOG, logging.DEBUG)


def main():
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    print(VolumeList.get_all(ctxt))


if __name__ == '__main__':
    main()
