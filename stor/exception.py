#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import six

from stor.i18n import _


logger = logging.getLogger(__name__)


class StorException(Exception):
    """Base Stor Exception

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.

    """
    message = _("An unknown exception occurred.")
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if message:
            self.message = message

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        for k, v in self.kwargs.items():
            if isinstance(v, Exception):
                self.kwargs[k] = six.text_type(v)

        try:
            message = self.message % kwargs
        except Exception:
            # NOTE(melwitt): This is done in a separate method so it can be
            # monkey-patched during testing to make it a hard failure.
            self._log_exception()
            message = self.message

        self.msg = message
        super(StorException, self).__init__(message)

    def _log_exception(self):
        # kwargs doesn't match a variable in the message
        # log the issue and the kwargs
        logger.exception('Exception in string format operation:')
        logger.error(self.__name__)
        for name, value in self.kwargs.items():
            logger.error("%(name)s: %(value)s",
                         {'name': name, 'value': value})

    def __unicode__(self):
        return self.msg
