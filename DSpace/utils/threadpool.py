#!/usr/bin/env python
# -*- coding: utf-8 -*-
from concurrent import futures

from oslo_log import log as logging

from DSpace.common.config import CONF
from DSpace.context import RequestContext

logger = logging.getLogger(__name__)


class TheadPoolMixin(object):
    def __init__(self, *args, **kwargs):
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)

    def _wapper(self, fun, *args, **kwargs):
        permanent = kwargs.pop("permanent", False)
        while True:
            try:
                # update ctxt
                if len(args) > 0 and isinstance(args[0], RequestContext):
                    ctxt = args[0]
                    ctxt.update_store()
                # run fun
                fun(*args, **kwargs)
            except Exception as e:
                logger.exception("Unexpected exception: %s", e)
            logger.info("fun %s permanent is %s", fun, permanent)
            if not permanent:
                break

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)
