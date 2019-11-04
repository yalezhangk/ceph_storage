#!/usr/bin/env python
# -*- coding: utf-8 -*-

from jinja2 import Environment
from jinja2 import PackageLoader

env = Environment(
    loader=PackageLoader('DSpace', 'templates'),
)


def get(name):
    return env.get_template(name)
