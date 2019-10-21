import base64
import datetime
import json
import logging
from tempfile import TemporaryFile

from truepy import License

LOG = logging.getLogger(__name__)
CA_FILE_PATH = '/etc/t2stor/license/certificate.pem'
PRIVATE_FILE = '/etc/t2stor/license/private-key.pem'
LICENSE_PASSWORD = 'nPgyLwljRy#1OdYd'


class LicenseVerify(object):
    def __init__(self, content=None):
        self._licenses_data = None
        self.content = content

    @property
    def licenses_node_number(self):
        if not self.licenses_data:
            return 0
        extra = json.loads(self.licenses_data.extra)
        licenses_node = extra.get('node', 0)
        return int(licenses_node)

    @property
    def licenses_data(self):
        try:
            data = self.load_licenses()
        except Exception:
            LOG.error('load license data error')
            data = None
        return data

    @property
    def license_cluster_size(self):
        if not self.licenses_data:
            return 0
        extra = json.loads(self.licenses_data.extra)
        size = extra.get('other_extra')
        if size:
            size = size.split(':')[1]
        else:
            size = 0
        return size

    @property
    def fact_cluster_size(self):
        # TODO:get fact_cluster_size,by ceph cmd or other
        return 0

    @property
    def fact_node_num(self):
        # TODO: get fact_node_num from db
        return 0

    @property
    def not_before(self):
        if not self.licenses_data:
            return None
        data = self.licenses_data.not_before.strftime('%Y-%m-%dT%H:%M:%S')
        return data

    @property
    def not_after(self):
        if not self.licenses_data:
            return None
        data = self.licenses_data.not_after.strftime('%Y-%m-%dT%H:%M:%S')
        return data

    def check_licenses_expiry(self):
        """
        检查licenses时间段是否在合法区间内
        """
        LOG.debug("开始检查licenses时间段")
        present_time = datetime.datetime.utcnow()
        if not self.licenses_data or self.licenses_data.not_after < \
                present_time\
                or self.licenses_data.not_before > present_time:
            LOG.error("licenses时间段不符")
            # todo 跳转到购买产品许可证页面
            return False
        else:
            return True

    def check_node_number(self):
        """
        检查节点数
        """
        LOG.debug("开始检查licenses授权节点数")
        licenses_node = self.licenses_node_number
        if licenses_node < self.fact_node_num:
            LOG.error('node节点数量超标')
        else:
            return True
        return False

    def check_size(self):
        total_size = self.license_cluster_size
        if int(total_size) < int(self.fact_cluster_size):
            return False
        return True

    def is_available(self):
        # 验证licese是否失效
        if False in [self.check_licenses_expiry(), self.check_node_number(),
                     self.check_size()]:
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
        self._licenses_data = lic.data
        return lic.data
