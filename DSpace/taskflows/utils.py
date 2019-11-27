#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_log import log as logging

from DSpace import objects

logger = logging.getLogger(__name__)


class CleanDataMixin(object):
    """Clean All Cluster Data"""

    def _clean_data(self, ctxt, cls, filters=None):
        objs = cls.get_all(ctxt, filters=filters)
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

    def _clean_ceph_config(self, ctxt):
        self._clean_data(ctxt, objects.CephConfigList)

    def _clean_volume_mapping(self, ctxt):
        self._clean_data(ctxt, objects.VolumeMappingList)

    def _clean_volume_access_path_gateway(self, ctxt):
        self._clean_data(ctxt, objects.VolumeAccessPathList)

    def _clean_volume_client(self, ctxt):
        self._clean_data(ctxt, objects.VolumeClientList)

    def _clean_volume_client_group(self, ctxt):
        self._clean_data(ctxt, objects.VolumeClientGroupList)

    def _clean_volume_gateway(self, ctxt):
        self._clean_data(ctxt, objects.VolumeGatewayList)

    def _clean_volume_access_path(self, ctxt):
        self._clean_data(ctxt, objects.VolumeAccessPathList)

    def _clean_volume_snapshot(self, ctxt):
        self._clean_data(ctxt, objects.VolumeSnapshotList)

    def _clean_volume(self, ctxt):
        self._clean_data(ctxt, objects.VolumeList)

    def _clean_rpc_service(self, ctxt):
        filters = {"cluster_id": ctxt.cluster_id}
        self._clean_data(ctxt, objects.RPCServiceList, filters=filters)

    def _clean_rack(self, ctxt):
        self._clean_data(ctxt, objects.RackList)

    def _clean_datacenter(self, ctxt):
        self._clean_data(ctxt, objects.DatacenterList)

    def _clean_sys_config(self, ctxt):
        self._clean_data(ctxt, objects.SysConfigList)

    def _clean_email_group(self, ctxt):
        self._clean_data(ctxt, objects.EmailGroupList)

    def _clean_alert_group(self, ctxt):
        self._clean_data(ctxt, objects.AlertGroupList)

    def _clean_alert_rule(self, ctxt):
        self._clean_data(ctxt, objects.AlertRuleList)

    def _clean_alert_log(self, ctxt):
        self._clean_data(ctxt, objects.AlertLogList)

    def _clean_action_log(self, ctxt):
        self._clean_data(ctxt, objects.ActionLogList)
