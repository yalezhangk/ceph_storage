from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import CompressionAlgorithm
from DSpace.objects.fields import PoolRole

logger = logging.getLogger(__name__)


class ObjectPolicyMixin(AdminBaseHandler):

    def check_object_policy_name_exist(self, ctxt, name):
        exist_policy = objects.ObjectPolicyList.get_all(
            ctxt, filters={'name': name})
        if exist_policy:
            raise exception.InvalidInput(
                reason=_('object_policy name %s already exists!') % name)

    def check_pool_and_compression(self, ctxt, data):
        # TODO: check  index_pool and data_pool
        index_pool = objects.Pool.get_by_id(ctxt, data['index_pool_id'])
        index_name = index_pool.pool_name
        if index_pool.role != PoolRole.INDEX:
            raise exception.Invalid(_('pool: %s is not index_pool') %
                                    index_pool.display_name)
        data_pool = objects.Pool.get_by_id(ctxt, data['data_pool_id'])
        data_name = data_pool.pool_name
        if data_pool.role != PoolRole.DATA:
            raise exception.Invalid(_('pool: %s is not data_pool') %
                                    data_pool.display_name)
        compression = data.get('compression')
        if compression not in CompressionAlgorithm.ALL:
            raise exception.CompressionAlgorithmNotFound(
                compression=compression)
        return {
            'index_pool_name': index_name,
            'data_pool_name': data_name,
            'compression': compression,
        }

    def check_del_object_policy(self, ctxt, policy):
        if policy.buckets:
            raise exception.Invalid(
                _('there are %s buckets using this policy'
                  ) % len(policy.buckets))

    def other_policies_unset_default(self, ctxt):
        other_policies = objects.ObjectPolicyList.get_all(
            ctxt, filters={'default': True})
        logger.info('other_policies: %s will unset default', other_policies)
        for policy in other_policies:
            policy.default = False
            policy.save()


class ObjectPolicyHandler(ObjectPolicyMixin):

    def object_policy_get_all(self, ctxt, marker=None, limit=None,
                              sort_keys=None, sort_dirs=None, filters=None,
                              offset=None, expected_attrs=None):
        return objects.ObjectPolicyList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)

    def object_policy_get_count(self, ctxt, filters=None):
        return objects.ObjectPolicyList.get_count(ctxt, filters=filters)

    def object_policy_create(self, ctxt, data):
        name = data.get('name')
        logger.debug('object_policy begin crate, name:%s' % name)
        self.check_object_policy_name_exist(ctxt, name)
        self.check_mon_host(ctxt)
        count = self.object_policy_get_count(ctxt)
        set_default = False if count else True
        extra_data = self.check_pool_and_compression(ctxt, data)
        policy_datas = {
            'name': name,
            'description': data['description'],
            'index_pool_id': data['index_pool_id'],
            'data_pool_id': data['data_pool_id'],
            'compression': extra_data['compression'],
            'cluster_id': ctxt.cluster_id
        }
        begin_action = self.begin_action(ctxt, Resource.OBJECT_POLICY,
                                         Action.CREATE)
        policy = objects.ObjectPolicy(ctxt, **policy_datas)
        policy.create()
        self.task_submit(self._object_policy_create, ctxt, policy, extra_data,
                         begin_action, set_default)
        logger.info('object_policy_create task has begin, policy_name: %s',
                    name)
        return policy

    def _object_policy_create(self, ctxt, policy, extra_data, begin_action,
                              set_default):
        name = policy.name
        node = self.get_first_mon_node(ctxt)
        self.check_agent_available(ctxt, node)
        try:
            # TODO: 改为 taskflow
            agent_client = self.agent_manager.get_client(node.id)
            agent_client.create_object_policy(
                ctxt, name, extra_data['index_pool_name'],
                extra_data['data_pool_name'],
                compression=extra_data['compression'])
            if set_default:
                agent_client.set_default_object_policy(ctxt, name)
                policy.default = True
                policy.save()
                logger.info('the first object_policy created and set_default '
                            'success, name=%s', name)
            agent_client.period_update(ctxt)
            logger.info('object_policy_create success, name=%s', name)
            op_status = "CREATE_SUCCESS"
            msg = _("create object_policy success: %s") % name
            err_msg = None
            status = 'success'
        except Exception as e:
            policy.destroy()
            logger.exception('object_policy_create error,name=%s,reason:%s',
                             name, str(e))
            op_status = "CREATE_ERROR"
            msg = _("create object_policy error: %s") % name
            err_msg = str(e)
            status = 'error'
        self.finish_action(begin_action, policy.id, policy.name, policy,
                           status, err_msg=err_msg)
        policy = objects.ObjectPolicy.get_by_id(ctxt, policy.id,
                                                joined_load=True)
        # send ws message
        self.send_websocket(ctxt, policy, op_status, msg)

    def policy_get_all_compressions(self, ctxt):
        return CompressionAlgorithm.ALL

    def object_policy_get(self, ctxt, object_policy_id, expected_attrs=None):
        object_policy = objects.ObjectPolicy.get_by_id(
            ctxt, object_policy_id, expected_attrs=expected_attrs)
        return object_policy

    def object_policy_update(self, ctxt, object_policy_id, data):
        logger.debug('object_policy: %s begin update description',
                     object_policy_id)
        policy = objects.ObjectPolicy.get_by_id(ctxt, object_policy_id)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_POLICY, Action.UPDATE, policy)
        policy.description = data['description']
        policy.save()
        logger.info('object_policy update success, new_description=%s',
                    data['description'])
        self.finish_action(begin_action, object_policy_id, policy.name, policy)
        return policy

    def object_policy_delete(self, ctxt, object_policy_id):
        policy = objects.ObjectPolicy.get_by_id(
            ctxt, object_policy_id, expected_attrs=['buckets'])
        logger.debug('object_policy begin delete: %s', policy.name)
        self.check_del_object_policy(ctxt, policy)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_POLICY, Action.DELETE)
        self.task_submit(self._object_policy_delete, ctxt, policy,
                         begin_action)
        logger.info('object_policy_delete task has begin, policy_name: %s',
                    policy.name)
        return policy

    def _object_policy_delete(self, ctxt, policy, begin_action):
        name = policy.name
        node = self.get_first_mon_node(ctxt)
        self.check_agent_available(ctxt, node)
        try:
            # TODO: 改为 taskflow
            agent_client = self.agent_manager.get_client(node.id)
            agent_client.delete_object_policy(ctxt, name)
            agent_client.period_update(ctxt)
            policy.destroy()
            logger.info('object_policy_delete success, name=%s', name)
            op_status = "DELETE_SUCCESS"
            msg = _("delete object_policy success: %s") % name
            err_msg = None
            status = 'success'
        except Exception as e:
            logger.exception('object_policy_delete error,name=%s,reason:%s',
                             name, str(e))
            op_status = "DELETE_ERROR"
            msg = _("delete object_policy error: %s") % name
            err_msg = str(e)
            status = 'error'
        self.finish_action(begin_action, policy.id, policy.name, policy,
                           status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, policy, op_status, msg)

    def object_policy_set_default(self, ctxt, object_policy_id):
        default_policy = objects.ObjectPolicy.get_by_id(ctxt, object_policy_id)
        if default_policy.default:
            raise exception.Invalid(_('is already default object_policy'))
        logger.debug('object_policy: %s begin set default',
                     default_policy.name)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_POLICY, Action.SET_DEFAULT)
        self.task_submit(self._object_policy_set_default, ctxt, default_policy,
                         begin_action)
        logger.debug('object_policy_set_default task has begin, policy_name: '
                     '%s', default_policy.name)
        return default_policy

    def _object_policy_set_default(self, ctxt, policy, begin_action):
        name = policy.name
        node = self.get_first_mon_node(ctxt)
        self.check_agent_available(ctxt, node)
        try:
            # 1. set default by agent
            agent_client = self.agent_manager.get_client(node.id)
            agent_client.set_default_object_policy(ctxt, name)
            # 2. period_update
            agent_client.period_update(ctxt)
            # 3. others unset default
            self.other_policies_unset_default(ctxt)
            policy.default = True
            policy.save()
            logger.info('object_policy set default success, name=%s', name)
            op_status = "SET_DEFAULT_SUCCESS"
            msg = _("set default object_policy success: %s") % name
            err_msg = None
            status = 'success'
        except Exception as e:
            logger.exception('object_policy set default error,name=%s,'
                             'reason:%s', name, str(e))
            op_status = "SET_DEFAULT_ERROR"
            msg = _("set default object_policy error: %s") % name
            err_msg = str(e)
            status = 'error'
        self.finish_action(begin_action, policy.id, policy.name, policy,
                           status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, policy, op_status, msg)

    def object_policy_set_compression(self, ctxt, policy_id, policy_data):
        policy = objects.ObjectPolicy.get_by_id(ctxt, policy_id)
        logger.debug('object_policy: %s begin set compression', policy.name)
        extra_data = {'old_compression': policy.compression}
        compression = policy_data.get('compression')
        if extra_data['old_compression'] == compression:
            logger.info('set_compression, compression unchanged, name=%s',
                        policy.name)
            return policy
        if compression not in CompressionAlgorithm.ALL:
            raise exception.CompressionAlgorithmNotFound(
                compression=compression)
        begin_action = self.begin_action(
            ctxt, Resource.OBJECT_POLICY, Action.SET_COMPRESSION)
        policy.compression = compression
        policy.save()
        self.task_submit(self._object_policy_set_compression, ctxt, policy,
                         extra_data, begin_action)
        logger.debug('object_policy_set_compression task has begin, '
                     'policy_name: %s', policy.name)
        return policy

    def _object_policy_set_compression(self, ctxt, policy, extra_data,
                                       begin_action):
        name = policy.name
        compression = policy.compression
        old_compression = extra_data['old_compression']
        node = self.get_first_mon_node(ctxt)
        self.check_agent_available(ctxt, node)
        try:
            agent_client = self.agent_manager.get_client(node.id)
            agent_client.modify_object_policy(
                ctxt, name, {'compression': compression})
            agent_client.period_update(ctxt)
            logger.info('object_policy set compression success, name=%s', name)
            op_status = "SET_COMPRESSION_SUCCESS"
            msg = _("set compression object_policy success: %s") % name
            err_msg = None
            status = 'success'
        except Exception as e:
            policy.compression = old_compression
            policy.save()
            logger.exception('object_policy set compression error,name=%s,'
                             'reason:%s', name, str(e))
            op_status = "SET_COMPRESSION_ERROR"
            msg = _("set compression object_policy error: %s") % name
            err_msg = str(e)
            status = 'error'
        self.finish_action(begin_action, policy.id, policy.name, policy,
                           status, err_msg=err_msg)
        # send ws message
        self.send_websocket(ctxt, policy, op_status, msg)
