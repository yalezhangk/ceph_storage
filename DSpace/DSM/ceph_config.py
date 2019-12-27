from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.utils import cluster_config

logger = logging.getLogger(__name__)


class CephConfigHandler(AdminBaseHandler):
    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        ceph_conf = objects.CephConfigList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return ceph_conf

    def ceph_config_get_count(self, ctxt, filters=None):
        return objects.CephConfigList.get_count(
            ctxt, filters=filters)

    def _get_mon_node(self, ctxt):
        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True}
        )
        return mon_nodes

    def _get_osd_node(self, ctxt, osd_name):
        if osd_name == "*":
            nodes = objects.NodeList.get_all(
                ctxt, filters={'cluster_id': ctxt.cluster_id,
                               'role_storage': True})
            return nodes
        else:
            osd = objects.OsdList.get_all(
                ctxt, filters={'osd_id': osd_name}, expected_attrs=['node'])[0]
            return [osd.node]

    def _get_all_node(self, ctxt):
        nodes = objects.NodeList.get_all(ctxt)
        return nodes

    def _get_config_nodes(self, ctxt, values):
        group = values['group']
        key = values['key']
        value = str(values['value'])

        nodes = []
        temp_configs = {}
        ceph_client = CephTask(ctxt)

        if group == 'global':
            if key in cluster_config.cluster_mon_restart_configs:
                nodes = self._get_mon_node(ctxt)
            if key in cluster_config.cluster_osd_restart_configs:
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in cluster_config.cluster_rgw_restart_configs:
                # TODO handle rgw
                pass
            if key in cluster_config.cluster_mon_temp_configs:
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_mon_node(ctxt)
            if key in cluster_config.cluster_osd_temp_configs:
                temp_configs = [{'service': 'osd.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in cluster_config.cluster_rgw_temp_configs:
                # TODO handle rgw
                pass

        if group.startswith('osd'):
            osd_id = group.split('.')
            if len(osd_id) == 1:
                if key in cluster_config.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.*',
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if len(osd_id) == 2:
                if key in cluster_config.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.' + osd_id[1],
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name=osd_id[1])

        if group == 'mon':
            if key in cluster_config.cluster_mon_temp_configs:
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
            nodes = self._get_mon_node(ctxt)

        if group == 'client':
            nodes = self._get_all_node(ctxt)

        if group == 'rgw':
            # TODO handle rgw
            pass

        if temp_configs:
            has_mon = self.has_monitor_host(ctxt)
            if not has_mon:
                return []
            ceph_client = CephTask(ctxt)
            try:
                ceph_client.config_set(temp_configs)
            except exception.CephException as e:
                logger.error(e)
                return []
        ceph_client.ceph_config()
        return nodes

    def _ceph_confg_update(self, ctxt, nodes, values):
        for n in nodes:
            node_task = NodeTask(ctxt, n)
            _success = node_task.ceph_config_update(ctxt, values)
            if not _success:
                logger.error(
                    'Ceph config update failed, node id: {}'.format(n)
                )
                return False
        return True

    def ceph_config_content(self, ctxt):
        content = objects.ceph_config.ceph_config_content(ctxt)
        return content

    def _ceph_config_db(self, ctxt, values):
        filters = {
            "group": values['group'],
            "key": values['key']
        }
        cephconf = objects.CephConfigList.get_all(ctxt, filters=filters)
        if not cephconf:
            cephconf = objects.CephConfig(
                ctxt, group=values.get('group'), key=values.get('key'),
                value=values.get('value'), value_type=values.get('value_type'),
                display_description=values.get('display_description'),
                cluster_id=ctxt.cluster_id
            )
            cephconf.create()
        else:
            cephconf = cephconf[0]
            cephconf.value = values.get('value')
            cephconf.save()
        return cephconf

    def _ceph_config_set(self, ctxt, values):
        nodes = self._get_config_nodes(ctxt, values)
        filters = {
            "group": values['group'],
            "key": values['key']
        }
        before_obj = objects.CephConfigList.get_all(ctxt, filters=filters)
        begin_action = self.begin_action(ctxt, Resource.CEPH_CONFIG,
                                         Action.UPDATE, before_obj=before_obj)
        if nodes:
            _success = self._ceph_confg_update(ctxt, nodes, values)
            msg = _('Ceph config update failed')
            if _success:
                cephconf = self._ceph_config_db(ctxt, values)
                msg = _('Ceph config update successful')
                op_status = "SET_CONFIG_SUCCESS"
            else:
                cephconf = {}
            status = 'success'
        else:
            cephconf = {}
            msg = _('Ceph config update failed')
            op_status = "SET_CONFIG_ERROR"
            status = 'fail'
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, cephconf, op_status, msg,
                               resource_type="CephConfig")
        self.finish_action(begin_action, None, Resource.CEPH_CONFIG,
                           after_obj=cephconf, status=status)

    def ceph_config_set(self, ctxt, values):
        self._ceph_config_set(ctxt, values)
        return values
