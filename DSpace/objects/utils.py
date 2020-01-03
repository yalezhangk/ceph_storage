#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

from DSpace import objects


def rack_create(ctxt):
    while True:
        name = "rack-%s" % str(uuid.uuid4())[0:8]
        racks = objects.RackList.get_all(ctxt, filters={"name": name})
        if racks:
            continue
        rack = objects.Rack(ctxt, name=name)
        rack.create()
        return rack


def datacenter_create(ctxt):
    while True:
        name = "datacenter-%s" % str(uuid.uuid4())[0:8]
        dcs = objects.DatacenterList.get_all(ctxt, filters={"name": name})
        if dcs:
            continue
        dc = objects.Datacenter(ctxt, name=name)
        dc.create()
        return dc


def rule_create(ctxt, rule_type):
    while True:
        name = "rule-%s" % str(uuid.uuid4())[0:6]
        crush_rules = objects.CrushRuleList.get_all(
            ctxt, filters={"rule_name": name})
        if crush_rules:
            continue
        crush_rule = objects.CrushRule(
            ctxt, rule_name=name, type=rule_type
        )
        crush_rule.create()
        return crush_rule
