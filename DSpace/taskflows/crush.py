#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

import six

from DSpace import exception
from DSpace import objects

logger = logging.getLogger(__name__)


class CrushContentGen(object):
    def __init__(self, ctxt, rule_name=None, root_name=None,
                 fault_domain='host', osds=None, content=None,
                 crush_rule_type='replicated'):
        self.ctxt = ctxt
        self.rule_name = rule_name
        self.root_name = root_name or rule_name
        self.fault_domain = fault_domain
        self.osds = osds
        self.crush_rule_type = crush_rule_type
        self.content = content or {
            "osds": {},
            "hosts": {},
            "racks": {},
            "datacenters": {},
        }

    @classmethod
    def from_content(cls, ctxt, content, osds):
        return cls(
            ctxt=ctxt,
            fault_domain=content['fault_domain'],
            rule_name=content['crush_rule_name'],
            root_name=content['root_name'],
            osds=osds,
            content=content,
            crush_rule_type=content['crush_rule_type']
        )

    def _get_osd_info(self, osd):
        disk = objects.Disk.get_by_id(self.ctxt, osd.disk_id)
        osd_info = {
            "size": disk.size,
            "name": osd.osd_name,
            "id": osd.osd_id,
            "disk_type": osd.disk_type,
        }
        return osd_info

    def _get_node_info(self, name, node):
        node_crush = "{}-{}".format(self.root_name, node.hostname)
        node_info = {
            "name": name,
            "crush_name": node_crush,
            "osds": []
        }
        return node_info

    def _get_rack_info(self, name, rack):
        rack_crush = "{}-{}".format(self.root_name, "rack%s" % rack.id)
        rack_info = {
            "name": name,
            "crush_name": rack_crush,
            "hosts": []
        }
        return rack_info

    def _get_datacenter_info(self, name, dc):
        dc_crush = "{}-{}".format(self.root_name, "datacenter%s" % dc.id)
        dc_info = {
            "name": name,
            "crush_name": dc_crush,
            "racks": []
        }
        return dc_info

    def _add_osd_content(self):
        content = self.content
        for osd in self.osds:
            # add osd to crush
            if osd.osd_name not in content['osds']:
                osd_info = self._get_osd_info(osd)
                content['osds'][osd.osd_name] = osd_info

            # add node to crush
            node = objects.Node.get_by_id(self.ctxt, osd.node_id)
            node_name = node.hostname
            if node_name not in content['hosts']:
                node_info = self._get_node_info(node_name, node)
                content['hosts'][node_name] = node_info
            content['hosts'][node_name]["osds"].append(osd.osd_name)

            # add rack to crush
            if not node.rack_id:
                continue
            rack = objects.Rack.get_by_id(self.ctxt, node.rack_id)
            rack_name = str(rack.id)
            if rack_name not in content['racks']:
                rack_info = self._get_rack_info(rack_name, rack)
                content['racks'][rack_name] = rack_info
            rack_hosts = content['racks'][rack_name]['hosts']
            if node_name not in rack_hosts:
                rack_hosts.append(node_name)

            # add datacenter to crush
            if not rack.datacenter_id:
                continue
            dc = objects.Datacenter.get_by_id(self.ctxt, rack.datacenter_id)
            dc_name = str(dc.id)
            if dc_name not in content['datacenters']:
                dc_info = self._get_datacenter_info(dc_name, dc)
                content['datacenters'][dc_name] = dc_info
            dc_racks = content['datacenters'][dc_name]['racks']
            if rack_name not in dc_racks:
                dc_racks.append(rack_name)

    def _rm_useless_osd(self):
        # clean osd resource
        content = self.content
        rm_osds = []
        for osd_name, info in six.iteritems(content['osds']):
            _osds = list(filter(
                lambda o: o.osd_name == osd_name,
                self.osds
            ))
            if not _osds:
                rm_osds.append(osd_name)
        logger.debug("rm osds: %s", rm_osds)
        for osd_name in rm_osds:
            content['osds'].pop(osd_name)

    def _rm_useless_host(self):
        # clean host resource
        content = self.content
        rm_hosts = []
        for host_name, info in six.iteritems(content['hosts']):
            osd_names = info['osds']
            all_osd_names = content['osds'].keys()
            info['osds'] = list(set(osd_names) & set(all_osd_names))
            if not info['osds']:
                rm_hosts.append(host_name)
        logger.debug("rm hosts: %s", rm_hosts)
        for host_name in rm_hosts:
            content['hosts'].pop(host_name)

    def _rm_useless_rack(self):
        # clean rack resource
        content = self.content
        rm_racks = []
        for rack_name, info in six.iteritems(content['racks']):
            host_names = info['hosts']
            all_host_names = content['hosts'].keys()
            info['hosts'] = list(set(host_names) & set(all_host_names))
            if not info['hosts']:
                rm_racks.append(rack_name)
        logger.debug("rm racks: %s", rm_racks)
        for rack_name in rm_racks:
            content['racks'].pop(rack_name)

    def _rm_useless_datacenter(self):
        # clean datacenter resource
        content = self.content
        rm_dcs = []
        for dc_name, info in six.iteritems(content['datacenters']):
            rack_names = info['racks']
            all_rack_names = content['racks'].keys()
            info['racks'] = list(set(rack_names) & set(all_rack_names))
            if not info['racks']:
                rm_dcs.append(dc_name)
        logger.debug("rm dcs: %s", rm_dcs)
        for dc_name in rm_dcs:
            content['datacenters'].pop(dc_name)

    def _rm_useless_resource(self):
        self._rm_useless_osd()
        self._rm_useless_host()
        self._rm_useless_rack()
        self._rm_useless_datacenter()

    def gen_content(self):
        self._add_osd_content()
        self._rm_useless_resource()
        logger.info("gen crush content: %s", json.dumps(self.content))
        content = self.content
        content["fault_domain"] = self.fault_domain
        content["root_name"] = self.root_name
        content["crush_rule_name"] = self.rule_name
        content["crush_rule_type"] = self.crush_rule_type
        return content

    def _fix_host_name(self, exists_content):
        hosts = self.content['hosts']
        for host_crush_name, _osds in six.iteritems(exists_content['hosts']):
            if not _osds:
                raise exception.ProgrammingError(
                    reason="empty host not allowed")
            osd = _osds[0]
            logger.debug("find host by osd(%s): %s", osd, hosts)
            _hosts = list(filter(
                lambda h: osd in h['osds'],
                six.itervalues(hosts)
            ))
            if not _hosts:
                raise exception.ProgrammingError(
                    reason="host not found by osd(%s)" % osd)
            host = _hosts[0]
            host['crush_name'] = host_crush_name

    def _fix_rack_name(self, exists_content):
        hosts = self.content['hosts']
        racks = self.content['racks']
        for rack_crush_name, _hosts in six.iteritems(exists_content['racks']):
            if not _hosts:
                raise exception.ProgrammingError(
                    reason="empty rack not allowed")
            host_crush_name = _hosts[0]
            _hosts = list(filter(
                lambda h: host_crush_name == h['crush_name'],
                six.itervalues(hosts)
            ))
            if not _hosts:
                raise exception.ProgrammingError(
                    reason="host(%s) not found" % host_crush_name)
            host = _hosts[0]
            _racks = list(filter(
                lambda r: host['name'] in r['hosts'],
                six.itervalues(racks)
            ))
            if not _racks:
                raise exception.ProgrammingError(
                    reason="rack not found by host(%s)" % host['name'])
            rack = _racks[0]
            rack['crush_name'] = rack_crush_name

    def _fix_datacenter_name(self, exists_content):
        racks = self.content['racks']
        datacenters = self.content['datacenters']
        for dc_crush_name, _racks in six.iteritems(
                exists_content['datacenters']):
            if not _racks:
                raise exception.ProgrammingError(
                    reason="empty datacenter not allowed")
            rack_crush_name = _racks[0]
            _racks = list(filter(
                lambda r: rack_crush_name == r['crush_name'],
                six.itervalues(racks)
            ))
            if not _racks:
                raise exception.ProgrammingError(
                    reason="rack(%s) not found" % rack_crush_name)
            rack = _racks[0]
            _dcs = list(filter(
                lambda d: rack['name'] in d['racks'],
                six.itervalues(datacenters)
            ))
            if not _dcs:
                raise exception.ProgrammingError(
                    reason="datacenter not found by rack(%s)" % rack['name'])
            dc = _dcs[0]
            dc['crush_name'] = dc_crush_name

    def map_exists(self, exists_content):
        content = self.content
        logger.info("input content: %s", json.dumps(content))
        logger.info("exists content: %s", json.dumps(exists_content))
        self._fix_host_name(exists_content)
        self._fix_rack_name(exists_content)
        self._fix_datacenter_name(exists_content)
        logger.info("output content: %s", json.dumps(content))
        return self.content
