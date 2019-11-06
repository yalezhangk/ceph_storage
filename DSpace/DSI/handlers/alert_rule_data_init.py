#!/usr/bin/env python
# -*- coding: utf-8 -*-

from DSpace import objects

init_datas = [
    {
        'resource_type': 'cluster',
        'type': 'cluster_usage',
        'trigger_value': '> 80%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
    {
        'resource_type': 'node',
        'type': 'cpu_usage',
        'trigger_value': '> 80%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
    {
        'resource_type': 'node',
        'type': 'memory_usage',
        'trigger_value': '> 85%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
    {
        'resource_type': 'node',
        'type': 'sys_disk_usage',
        'trigger_value': '> 80%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
    {
        'resource_type': 'osd',
        'type': 'osd_usage',
        'trigger_value': '> 80%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
    {
        'resource_type': 'pool',
        'type': 'pool_usage',
        'trigger_value': '> 80%',
        'level': 'WARN',
        'trigger_period': '1440'
    },
]


def init_alert_rule(ctxt, cluster_id):
    for init_data in init_datas:
        init_data.update({'cluster_id': cluster_id})
        alert_rule = objects.AlertRule(ctxt, **init_data)
        alert_rule.create()
