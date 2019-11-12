#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

logger = logging.getLogger(__name__)


def get_file_content(path, default=None, strip=True):
    data = default
    if os.path.exists(path) and os.access(path, os.R_OK):
        try:
            try:
                datafile = open(path)
                data = datafile.read()
                if strip:
                    data = data.strip()
                if len(data) == 0:
                    data = default
            finally:
                datafile.close()
        except Exception:
            pass
    return data
