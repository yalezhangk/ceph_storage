from oslo_log import log as logging

from t2stor import exception
from t2stor import objects
from t2stor.admin.base import AdminBaseHandler
from t2stor.agent.client import AgentClientManager
from t2stor.api.wsclient import WebSocketClientManager
from t2stor.i18n import _
from t2stor.taskflows.ceph import CephTask
from t2stor.utils import cluster_config as ClusterConfg

logger = logging.getLogger(__name__)


class CephConfigHandler(AdminBaseHandler):
    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        ceph_conf = objects.CephConfigList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return ceph_conf

    def _get_mon_node(self, ctxt):
        mons = objects.ServiceList.get_all(
            ctxt, filters={'name': 'mon', 'cluster_id': ctxt.cluster_id})
        node = []
        for mon in mons:
            node.append(mon.node_id)
        return node

    def _get_osd_node(self, ctxt, osd_name):
        node = []
        if osd_name == "*":
            nodes = objects.NodeList.get_all(
                ctxt, filters={'cluster_id': ctxt.cluster_id,
                               'role_storage': True})
            for n in nodes:
                node.append(n.id)
        else:
            osd = objects.OsdList.get_all(
                ctxt, filters={
                    'osd_id': osd_name, 'cluster_id':
                    ctxt.cluster_id
                })[0]
            node.append(osd.id)
        return node

    def _get_all_node(self, ctxt):
        node = []
        nodes = objects.NodeList.get_all(
            ctxt, filters={'cluster_id': ctxt.cluster_id})
        for n in nodes:
            node.append(n.id)
        return node

    def _get_config_nodes(self, ctxt, values):
        group = values['group']
        key = values['key']
        value = values['value']

        nodes = []
        temp_configs = {}
        ceph_client = CephTask(ctxt)

        if group == 'global':
            if key in ClusterConfg.cluster_mon_restart_configs:
                nodes = self._get_mon_node(ctxt)
            if key in ClusterConfg.cluster_osd_restart_configs:
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in ClusterConfg.cluster_rgw_restart_configs:
                # TODO handle rgw
                pass
            if key in ClusterConfg.cluster_mon_temp_configs:
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_mon_node(ctxt)
            if key in ClusterConfg.cluster_osd_temp_configs:
                temp_configs = [{'service': 'osd.*',
                                 'key': key,
                                 'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if key in ClusterConfg.cluster_rgw_temp_configs:
                # TODO handle rgw
                pass

        if group.startswith('osd'):
            osd_id = group.split('.')
            if len(osd_id) == 1:
                if key in ClusterConfg.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.*',
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name='*')
            if len(osd_id) == 2:
                if key in ClusterConfg.cluster_osd_temp_configs:
                    temp_configs = [{'service': 'osd.' + osd_id[1],
                                     'key': key,
                                     'value': value}]
                nodes = self._get_osd_node(ctxt, osd_name=osd_id[1])

        if group == 'mon':
            if key in ClusterConfg.cluster_mon_temp_configs:
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
            client = AgentClientManager(
                ctxt, cluster_id=ctxt.cluster_id).get_client(node_id=n)
            _success = client.ceph_config_update(ctxt, values)
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
                value=values.get('value'),
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
        if nodes:
            _success = self._ceph_confg_update(ctxt, nodes, values)
            msg = _('Ceph config update failed')
            if _success:
                self._ceph_config_db(ctxt, values)
                msg = _('Ceph config update successful')
        else:
            msg = _('Ceph config update failed')

        # send ws message
        wb_client = WebSocketClientManager(
            context=ctxt, cluster_id=ctxt.cluster_id).get_client()
        wb_client.send_message(ctxt, values, "UPDATED", msg)

    def ceph_config_set(self, ctxt, values):
        self.executor.submit(self._ceph_config_set(ctxt, values))
        return values