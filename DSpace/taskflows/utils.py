#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_log import log as logging

from DSpace import objects

logger = logging.getLogger(__name__)


class CleanDataMixin(object):
    """Clean All Cluster Data"""

    def _clean_data(self, ctxt, cls):
        objs = cls.get_all(ctxt)
        for obj in objs:
            obj.destroy()

    def _clean_pool(self, ctxt):
        self._clean_data(ctxt, objects.PoolList)

    def _clean_crush_rule(self, ctxt):
        self._clean_data(ctxt, objects.CrushRuleList)

    def _clean_osd(self, ctxt):
        self._clean_data(ctxt, objects.OsdList)

    def _clean_disk_partition(self, ctxt):
        self._clean_data(ctxt, objects.DiskPartitionList)

    def _clean_disk(self, ctxt):
        self._clean_data(ctxt, objects.DiskList)

    def _clean_network(self, ctxt):
        self._clean_data(ctxt, objects.NetworkList)

    def _clean_node(self, ctxt):
        self._clean_data(ctxt, objects.NodeList)
