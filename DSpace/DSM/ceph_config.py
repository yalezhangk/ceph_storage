import json

from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask

logger = logging.getLogger(__name__)
CEPH_CONFS = json.load(open(CONF.ceph_confs_path, 'r'))


class CephConfigHandler(AdminBaseHandler):
    def ceph_config_get_all(
            self, ctxt, marker=None, limit=None, sort_keys=None,
            sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        filters['group'] = objects.CephConfig.Not("keyring")
        ceph_conf = objects.CephConfigList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        return ceph_conf, CEPH_CONFS

    def ceph_config_get_count(self, ctxt, filters=None):
        return objects.CephConfigList.get_count(
            ctxt, filters=filters)

    def _get_rgw_node(self, ctxt):
        rgw_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_object_gateway": True}
        )
        return rgw_nodes

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
        temp_configs = []
        ceph_client = CephTask(ctxt)

        conf = CEPH_CONFS.get(values['key'])

        if group == 'global':
            if 'mon' in conf.get('owner'):
                nodes += list(self._get_mon_node(ctxt))
                if not conf.get('need_restart'):
                    temp_configs += [{'service': 'mon.*',
                                      'key': key,
                                      'value': value}]
            elif 'mds' in conf.get('owner'):
                nodes += list(self._get_mon_node(ctxt))
            elif 'mgr' in conf.get('owner'):
                nodes += list(self._get_mon_node(ctxt))

            if 'osd' in conf.get('owner'):
                nodes += list(self._get_osd_node(ctxt, osd_name='*'))
                if not conf.get('need_restart'):
                    temp_configs += [{'service': 'osd.*',
                                      'key': key,
                                      'value': value}]
        elif group.startswith('osd'):
            osd_id = group.split('.')
            if len(osd_id) == 1:
                nodes = self._get_osd_node(ctxt, osd_name='*')
                if not conf.get('need_restart'):
                    temp_configs = [{'service': 'osd.*',
                                     'key': key,
                                     'value': value}]
            elif len(osd_id) == 2:
                nodes = self._get_osd_node(ctxt, osd_name=osd_id[1])
                if not conf.get('need_restart'):
                    temp_configs = [{'service': 'osd.' + osd_id[1],
                                     'key': key,
                                     'value': value}]
        elif group == 'mon':
            nodes = self._get_mon_node(ctxt)
            if not conf.get('need_restart'):
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
        elif group == 'mds':
            nodes = self._get_mon_node(ctxt)
        elif group == 'mgr':
            nodes = self._get_mon_node(ctxt)
        else:
            logger.error("group %s not support now", group)
            return []

        if len(temp_configs):
            has_mon = self.has_monitor_host(ctxt)
            if not has_mon:
                return []
            ceph_client = CephTask(ctxt)
            try:
                ceph_client.config_set(temp_configs)
            except exc.CephException as e:
                logger.error(e)
                return []
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

    def _ceph_config_set(self, ctxt, values, begin_action):
        try:
            nodes = self._get_config_nodes(ctxt, values)
            if nodes:
                _success = self._ceph_confg_update(ctxt, nodes, values)
                msg = (_('Ceph config %s update failed') % values['key'])
                if _success:
                    cephconf = self._ceph_config_db(ctxt, values)
                    msg = (_('Ceph config %s update success') % values['key'])
                    op_status = "SET_CONFIG_SUCCESS"
                else:
                    cephconf = {}
                status = 'success'
            else:
                cephconf = {}
                msg = (_('Ceph config %s update failed') % values['key'])
                op_status = "SET_CONFIG_ERROR"
                status = 'fail'
        except Exception as e:
            logger.exception("update ceph config %s failed"
                             ": %s", values['key'], e)
            cephconf = {}
            msg = (_('Ceph config %s update failed') % values['key'])
            op_status = "SET_CONFIG_ERROR"
            status = 'fail'
        # send ws message
        self.send_websocket(ctxt, cephconf, op_status, msg,
                            resource_type="CephConfig")
        self.finish_action(begin_action, None, Resource.CEPH_CONFIG,
                           after_obj=cephconf, status=status)

    def _config_check(self, group, key):
        conf = CEPH_CONFS.get(key)
        if not conf:
            raise exc.InvalidInput(_("modify conf %s not support") % key)
        if group.startswith('osd') and 'osd' not in conf.get('owner'):
            raise exc.InvalidInput(_("conf %s not in %s group") % (key, 'osd'))
        elif group == 'mon' and 'mon' not in conf.get('owner'):
            raise exc.InvalidInput(_("conf %s not in %s group") % (key, 'mon'))
        elif group == 'mds' and 'mds' not in conf.get('owner'):
            raise exc.InvalidInput(_("conf %s not in %s group") % (key, 'mds'))
        elif group == 'mgr' and 'mgr' not in conf.get('owner'):
            raise exc.InvalidInput(_("conf %s not in %s group") % (key, 'mgr'))

    def ceph_config_set(self, ctxt, values):
        self._config_check(values['group'], values['key'])
        filters = {
            "group": values['group'],
            "key": values['key']
        }
        before_obj = objects.CephConfigList.get_all(ctxt, filters=filters)
        if not before_obj:
            before_obj = {}
        else:
            before_obj = before_obj[0]
        begin_action = self.begin_action(ctxt, Resource.CEPH_CONFIG,
                                         Action.UPDATE, before_obj=before_obj)
        self.task_submit(self._ceph_config_set, ctxt, values, begin_action)
        return values

    def _get_remove_nodes(self, ctxt, conf):
        group = conf.group
        key = conf.key
        nodes = []
        temp_configs = []
        ceph_client = CephTask(ctxt)
        default_conf = CEPH_CONFS.get(key)
        value = default_conf.get('default')

        filters = {
            "group": 'global',
            "key": key
        }
        global_conf = objects.CephConfigList.get_all(ctxt, filters=filters)
        if global_conf:
            global_conf = global_conf[0]

        if group == 'global':
            if 'mon' in default_conf.get('owner'):
                nodes += list(self._get_mon_node(ctxt))
                if not default_conf.get('need_restart'):
                    temp_configs += [{'service': 'mon.*',
                                      'key': key,
                                      'value': value}]
            if 'osd' in default_conf.get('owner'):
                nodes += list(self._get_osd_node(ctxt, osd_name='*'))
                if not default_conf.get('need_restart'):
                    temp_configs += [{'service': 'osd.*',
                                      'key': key,
                                      'value': value}]
        elif group.startswith('osd'):
            osd_id = group.split('.')
            if global_conf:
                value = str(global_conf.value)
            if len(osd_id) == 1:
                nodes = self._get_osd_node(ctxt, osd_name='*')
                if not default_conf.get('need_restart'):
                    temp_configs = [{'service': 'osd.*',
                                     'key': key,
                                     'value': value}]
            elif len(osd_id) == 2:
                nodes = self._get_osd_node(ctxt, osd_name=osd_id[1])
                if not default_conf.get('need_restart'):
                    temp_configs = [{'service': 'osd.' + osd_id[1],
                                     'key': key,
                                     'value': value}]
        elif group == 'mon':
            if global_conf:
                value = str(global_conf.value)
            nodes = self._get_mon_node(ctxt)
            if not default_conf.get('need_restart'):
                temp_configs = [{'service': 'mon.*',
                                 'key': key,
                                 'value': value}]
        else:
            logger.error("not support group %s", group)
            return []

        if len(temp_configs):
            has_mon = self.has_monitor_host(ctxt)
            if not has_mon:
                return []
            ceph_client = CephTask(ctxt)
            try:
                ceph_client.config_set(temp_configs)
            except exc.CephException as e:
                logger.error(e)
                return []
        return nodes

    def _ceph_conf_remove(self, ctxt, nodes, conf):
        for n in nodes:
            node_task = NodeTask(ctxt, n)
            _success = node_task.ceph_config_remove(ctxt, conf)
            if not _success:
                logger.error(
                    'Ceph config remove failed, node id: {}'.format(n)
                )
                return False
        return True

    def _ceph_config_remove(self, ctxt, conf, begin_action):
        try:
            nodes = self._get_remove_nodes(ctxt, conf)
            if nodes:
                _success = self._ceph_conf_remove(ctxt, nodes, conf)
                if _success:
                    conf.destroy()
                    msg = (_('Ceph config %s remove success') % conf.key)
                    op_status = "REMOVE_CONFIG_SUCCESS"
                    status = 'success'
                else:
                    msg = (_('Ceph config %s remove failed') % conf.key)
                    status = 'fail'
                    op_status = "REMOVE_CONFIG_ERROR"
            else:
                msg = (_('Ceph config %s remove failed') % conf.key)
                op_status = "REMOVE_CONFIG_ERROR"
                status = 'fail'
        except Exception as e:
            logger.exception("remove ceph config %s failed"
                             ": %s", conf.key, e)
            msg = (_('Ceph config %s remove failed') % conf.key)
            op_status = "REMOVE_CONFIG_ERROR"
            status = 'fail'
        # send ws message
        self.send_websocket(ctxt, conf, op_status, msg,
                            resource_type="CephConfig")
        self.finish_action(begin_action, None, Resource.CEPH_CONFIG,
                           after_obj=None, status=status)

    def ceph_config_remove(self, ctxt, config_id):
        conf = objects.CephConfig.get_by_id(ctxt, config_id)
        self._config_check(conf.group, conf.key)
        begin_action = self.begin_action(ctxt, Resource.CEPH_CONFIG,
                                         Action.UPDATE, before_obj=conf)
        self.task_submit(self._ceph_config_remove, ctxt, conf, begin_action)
        return conf
