import base64
import datetime
import json
import logging
from tempfile import TemporaryFile

from truepy import License

from DSpace import exception as exc
from DSpace import objects
from DSpace.context import get_context
from DSpace.DSM.client import AdminClientManager
from DSpace.i18n import _
from DSpace.objects.fields import ConfigKey
from DSpace.tools.utils import utc2local_time

LOG = logging.getLogger(__name__)
CA_FILE_PATH = '/etc/dspace/license/certificate.pem'
PRIVATE_FILE = '/etc/dspace/license/private-key.pem'
LICENSE_PASSWORD = 'default-pwd'
LICENSE_FREE_TIME = 24 * 60 * 60


def skip_license_verify():
    # 1. 开关license
    # 2. 是否24h免费
    disable_license = objects.sysconfig.sys_config_get(
        get_context(), ConfigKey.DISABLE_LICENSE)
    if disable_license is True:
        LOG.info('disable_license is true, will skip license')
        return True
    # 校验是否24h免费
    init_portal_time = objects.sysconfig.sys_config_get(
        get_context(), ConfigKey.INIT_PORTAL_TIME)
    if not init_portal_time:
        LOG.info('not set license free time, will verify license')
        return False
    # init_portal_time 是utc时间
    init_portal_time = datetime.datetime.strptime(init_portal_time,
                                                  '%Y-%m-%d %H:%M:%S')
    now_time = datetime.datetime.utcnow()
    time_diff = (now_time - init_portal_time).total_seconds()
    if time_diff < LICENSE_FREE_TIME:
        LOG.info('license in free time:%sS, time_diff:%sS, will skip '
                 'license_verify' % (LICENSE_FREE_TIME, time_diff))
        return True
    LOG.debug('license: %s more than free time: %s, will verify license'
              % (time_diff, LICENSE_FREE_TIME))
    return False


class LicenseVerifyTool(object):

    def __init__(self):
        self.ctxt = get_context()
        self.is_skip = skip_license_verify()

    def get_license_verify_tool(self):
        licenses = objects.LicenseList.get_all(self.ctxt)
        if not licenses:
            raise exc.InvalidInput(_("not license file, please upload "
                                     "license file"))
        else:
            verify_tool = LicenseVerify(licenses[0].content, self.ctxt)
            if not verify_tool.licenses_data:
                raise exc.InvalidInput(_("license verify fail, please upload "
                                         "authorized license file"))
        return verify_tool


class LicenseVerify(object):
    _licenses_data = None

    def __init__(self, content=None, ctxt=None):
        self._licenses_data = None
        self._extra_data = None
        self.content = content
        self.ctxt = ctxt

    def get_admin_client(self, cluster_id):
        client = AdminClientManager(
            self.ctxt,
            async_support=False
        ).get_client()
        return client

    @property
    def licenses_node_number(self):
        if not self._licenses_data:
            return 0
        licenses_node = self._extra_data.get('node', 0)
        return int(licenses_node)

    @property
    def licenses_data(self):
        if self._licenses_data:
            return self._licenses_data
        try:
            data = self.load_licenses()
            self._licenses_data = data
            self._extra_data = json.loads(data.extra)
        except Exception:
            LOG.error('load license data error')
            data = None
        return data

    @property
    def license_cluster_size(self):
        # 返回license授权容量,bytes
        if not self._licenses_data:
            return 0
        size = self._extra_data.get('capacity')
        if size and size.isdigit():
            # capacity is GB
            size = int(size) * (1024 ** 3)
        else:
            size = 0
        return size

    def max_cluster_size(self):
        cluters = objects.ClusterList.get_all(self.ctxt)
        max_size = 0
        for cluster in cluters:
            client = self.get_admin_client(cluster.id)
            cluster_info = client.ceph_cluster_info(self.ctxt)
            size = int(cluster_info.get('total_cluster_byte', 0))
            if size > max_size:
                max_size = size
            else:
                continue
        return max_size

    def max_node_num(self):
        cluters = objects.ClusterList.get_all(self.ctxt)
        max_num = 0
        for cluster in cluters:
            nodes = objects.NodeList.get_all(
                self.ctxt, filters={"cluster_id": cluster.id})
            if len(nodes) > max_num:
                max_num = len(nodes)
            else:
                continue
        return max_num

    def all_cluster_size(self):
        # total_size_bytes
        osds = objects.OsdList.get_all(
            self.ctxt, filters={"cluster_id": "*"})
        total_size_bytes = 0
        for osd in osds:
            total_size_bytes += osd.size
        return total_size_bytes

    def all_node_num(self):
        # total node num
        nodes = objects.NodeList.get_all(
            self.ctxt, filters={"cluster_id": "*"})
        return len(nodes)

    def not_before_format(self):
        # format datetime
        if not self.licenses_data:
            return None
        return utc2local_time(self.licenses_data.not_before)

    def not_after_format(self):
        # format datetime
        if not self.licenses_data:
            return None
        return utc2local_time(self.licenses_data.not_after)

    def check_licenses_expiry(self):
        """
        检查license时间段是否在区间内
        """
        LOG.debug("开始检查license时间段")
        present_time = datetime.datetime.utcnow()
        not_after = self.licenses_data.not_after
        not_before = self.licenses_data.not_before
        if not self.licenses_data or not_after < present_time or \
                not_before > present_time:
            LOG.error("license时间段不符, 当前时间: %s, 激活时间: %s, 截止时间: %s"
                      % (present_time, not_before, not_after))
            return False
        return True

    def check_node_number(self, add_node_num=None):
        """
        检查节点数
        add_node_num, 待添加的节点数量，int
        :return:
        available: 节点数是否超标，
        authorize_node_num: 已授权的节点数，
        fact_node_num: 所有集群实际总节点数
        """
        LOG.debug("开始检查licenses授权节点数")
        licenses_node = self.licenses_node_number
        fact_node_num = self.all_node_num()
        if add_node_num:
            fact_node_num += add_node_num
        result = {
            'available': False,
            'authorize_node_num': licenses_node,
            'fact_node_num': fact_node_num
        }
        if licenses_node < fact_node_num:
            LOG.error('license verify: 节点数量超标, 已授权: %s 个, 当前集群: %s 个'
                      % (licenses_node, fact_node_num))
        else:
            result['available'] = True
        return result

    def check_size(self, add_size=None):
        """
        检查集群容量
        add_size,待添加的容量，int, bytes
        :return:
        available: 大小是否超标，
        authorize_size: 已授权的容量，
        fact_size: 所有集群实际总容量
        """
        LOG.debug("开始检查licenses集群容量")
        license_size = self.license_cluster_size
        fact_size = self.all_cluster_size()
        if add_size:
            fact_size += add_size
        result = {
            'available': False,
            'authorize_size': license_size,
            'fact_size': fact_size
        }
        if license_size < fact_size:
            LOG.error('license verify: 集群容量超标, 已授权: %s, 集群容量: %s'
                      % (license_size, fact_size))
        else:
            result['available'] = True
        return result

    def is_available(self):
        # 验证licese是否可用
        LOG.debug('check license is_available: expiry_time, size, node_num')
        if False in [self.check_licenses_expiry(),
                     self.check_node_number()['available'],
                     self.check_size()['available']]:
            return False
        return True

    def load_licenses(self):
        content = base64.b64decode(self.content)
        lice_file = TemporaryFile()
        lice_file.write(content)
        lice_file.seek(0)
        lic = License.load(lice_file, LICENSE_PASSWORD.encode('utf-8'))
        # Load the certificate
        with open(CA_FILE_PATH, 'rb') as f:
            certificate = f.read()
        # Verify the license; this will raise
        # License.InvalidSignatureException if,the signature is incorrect
        lic.verify(certificate)
        lice_file.close()
        return lic.data
