import logging

from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.docker import DockerSocket as DockerSockTool
from DSpace.tools.radosgw import RadosgwTool
from DSpace.tools.radosgw_admin import RadosgwAdmin

logger = logging.getLogger(__name__)


class RgwMetricHandler(AgentBaseHandler):

    def get_rgw_obj_user_capacity(self, ctxt, access_key, secret_key, server,
                                  uid):
        rgw_admin = RadosgwAdmin(access_key, secret_key, server)
        result = rgw_admin.get_user_capacity(uid)
        return result

    def get_rgw_user_capacity(self, ctxt, access_key, secret_key,
                              server, uid):
        result = self.get_rgw_obj_user_capacity(ctxt, access_key, secret_key,
                                                server, uid)
        size_used = result.get('size').get('used')
        size_max = result.get('size').get('max')
        if int(size_max) == 0:
            # if quota is unlimited, return -1
            size_max = -1
        obj_num = result.get('objects').get('used')
        return {'uid': uid,
                'size_used': size_used,
                'obj_num': obj_num,
                'kb_size_total': size_max
                }

    def get_rgw_user_usage(self, ctxt, access_key, secret_key, server, uid):
        rgw_admin = RadosgwAdmin(access_key, secret_key, server)
        categories = rgw_admin.get_user_usage(uid)
        bytes_sent = 0
        sent_ops = 0
        bytes_received = 0
        received_ops = 0
        delete_ops = 0
        for data in categories:
            category = data['category']
            if category == 'put_obj':
                bytes_sent = data['bytes_received']
                sent_ops = data['ops']
            elif category == 'get_obj':
                bytes_received = data['bytes_sent']
                received_ops = data['ops']
            elif category == 'delete_obj':
                delete_ops = data['ops']
        return {
            'bytes_sent': bytes_sent,
            'sent_ops': sent_ops,
            'bytes_received': bytes_received,
            'received_ops': received_ops,
            'delete_ops': delete_ops,
        }

    def get_all_rgw_buckets_capacity(self, ctxt, access_key, secret_key,
                                     server):
        rgw_admin = RadosgwAdmin(access_key, secret_key, server)
        results = rgw_admin.get_bucket_stats()
        datas = []
        for data in results:
            bucket_data = {'bucket': data['bucket'], 'owner': data['owner']}
            usage = data['usage'].get('rgw.main', {})
            bucket_quota = data['bucket_quota']
            max_size_kb = bucket_quota['max_size_kb']
            if int(max_size_kb) == 0:
                # if quota is unlimited, return -1
                max_size_kb = -1
            bucket_data['bucket_kb_total'] = max_size_kb
            bucket_data['bucket_kb_used'] = usage.get('size_kb_actual', 0)
            bucket_data['bucket_object_num'] = usage.get('num_objects', 0)
            datas.append(bucket_data)
        logger.debug('get_all_rgw_buckets_capacity:%s', datas)
        return datas

    def get_all_rgw_buckets_usage(self, ctxt, access_key, secret_key, server):
        rgw_admin = RadosgwAdmin(access_key, secret_key, server)
        entries = rgw_admin.get_all_buckets_usage()
        datas = []
        for user in entries:
            owner = user['user']
            buckets = user['buckets']
            for bucket_data in buckets:
                bucket = bucket_data['bucket']
                data = {'owner': owner, 'bucket': bucket}
                categories = bucket_data['categories']
                bytes_sent = 0
                sent_ops = 0
                bytes_received = 0
                received_ops = 0
                delete_ops = 0
                for categorie_data in categories:
                    category = categorie_data['category']
                    if category == 'put_obj':
                        bytes_sent = categorie_data['bytes_received']
                        sent_ops = categorie_data['ops']
                    elif category == 'get_obj':
                        bytes_received = categorie_data['bytes_sent']
                        received_ops = categorie_data['ops']
                    elif category == 'delete_obj':
                        delete_ops = categorie_data['ops']
                data.update({'bytes_sent': bytes_sent,
                             'sent_ops': sent_ops,
                             'bytes_received': bytes_received,
                             'received_ops': received_ops,
                             'delete_ops': delete_ops
                             })
                datas.append(data)
        logger.debug('get_all_rgw_buckets_usage:%s', datas)
        return datas

    def get_rgw_gateway_cup_memory(self, ctxt, rgw_names):
        rgw_tool = RadosgwTool(self._get_ssh_executor())
        results = rgw_tool.get_rgw_gateway_cup_and_memory(rgw_names)
        return results

    def get_rgw_router_cup_memory(self, ctxt):
        docket_socket_tool = DockerSockTool(self._get_executor())
        container_keepalived = '{}_radosgw_keepalived'.format(
            self.container_prefix)
        container_haproxy = '{}_radosgw_haproxy'.format(self.container_prefix)
        result_keep = docket_socket_tool.stats(container_keepalived)
        result_ha = docket_socket_tool.stats(container_haproxy)
        return result_keep, result_ha
