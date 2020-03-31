import logging

from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.radosgw_admin import RadosgwAdmin

logger = logging.getLogger(__name__)


class RgwMetricHandler(AgentBaseHandler):

    def get_rgw_user_capacity(self, ctxt, access_key, secret_key, server, uid):
        rgw_admin = RadosgwAdmin(access_key, secret_key, server)
        result = rgw_admin.get_user_capacity(uid)
        logger.debug('get_rgw_user_capacity:%s', result)
        return result

    def get_rgw_user_kb_used_and_obj_num(self, ctxt, access_key, secret_key,
                                         server, uid):
        result = self.get_rgw_user_capacity(ctxt, access_key, secret_key,
                                            server, uid)
        size_used = result.get('size').get('used')
        obj_num = result.get('objects').get('used')
        return {'cluster_id': ctxt.cluster_id,
                'uid': uid,
                'size_used': size_used,
                'obj_num': obj_num
                }
