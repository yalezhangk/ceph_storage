import logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.utils.license_verify import LicenseVerify

logger = logging.getLogger(__name__)


class LicenseHandler(AdminBaseHandler):

    def license_get_all(self, ctxt):
        # unauthorized:未授权，authorized:已授权， lapsed:已失效
        licenses = objects.LicenseList.get_all(ctxt)
        has_cluster = objects.ClusterList.get_all(ctxt)
        if has_cluster:
            logging.info('try get cluster size, cluster_id:%s',
                         ctxt.cluster_id)
            cluster_info = self.ceph_cluster_info(ctxt)
            size = int(cluster_info.get('total_cluster_byte', 0))
            logger.info('try get current cluster:%s node_num', ctxt.cluster_id)
            node_num = self.node_get_count(ctxt)
        else:
            logger.info('not yet cluster_id,'
                        'cluser_size and node_num default is 0')
            size = 0
            node_num = 0
        result = {'license': []}
        if licenses:
            for per_license in licenses:
                v = LicenseVerify(per_license.content, ctxt)
                if not v.licenses_data:
                    logger.error('load license data error')
                    result['license'].append({'status': 'unauthorized'})
                else:
                    is_available = v.is_available()
                    if per_license.status == 'lapsed':
                        status = 'lapsed'
                    else:
                        status = 'authorized' if is_available else 'lapsed'
                    up_data = {
                        'id': per_license.id,
                        'status': status,
                        'not_before': v.not_before,
                        'not_after': v.not_after,
                        'product': 'T2STOR',
                        'fact_size': size,
                        'size': v.license_cluster_size,
                        'fact_node_num': node_num,
                        'node_num': v.licenses_node_number
                    }
                    result['license'].append(up_data)
        return result

    def upload_license(self, ctxt, content):
        result = {'license': {'result': False, 'msg': None}}
        license = objects.License(ctxt, content=content, status='valid')
        begin_action = self.begin_action(
            ctxt, Resource.LICENSE, Action.UPLOAD_LICENSE)
        # license_verify校验
        v = LicenseVerify(license.content, ctxt)
        if not v.licenses_data:
            license.status = 'invalid'
            err_msg = _('the license.key is invalid')
            self.finish_action(begin_action, None, 'license', after_obj=None,
                               status='fail', err_msg=err_msg)
            raise InvalidInput(reason=err_msg)
        else:
            result['license']['result'] = True
            # 其他的改为已失效
            licenses = objects.LicenseList.get_all(ctxt)
            for licen in licenses:
                licen.status = 'lapsed'
                licen.save()
        license.create()
        self.finish_action(begin_action, license.id, 'license', license,
                           'success')
        return result
