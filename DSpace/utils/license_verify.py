import base64
import datetime
import json
import logging
from tempfile import TemporaryFile

from truepy import License

from DSpace import objects
from DSpace.DSM.client import AdminClientManager

LOG = logging.getLogger(__name__)
CA_FILE_PATH = '/etc/dspace/license/certificate.pem'
PRIVATE_FILE = '/etc/dspace/license/private-key.pem'
LICENSE_PASSWORD = 'nPgyLwljRy#1OdYd'


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
            cluster_id=cluster_id,
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
        if not self._licenses_data:
            return 0
        size = self._extra_data.get('other_extra')
        if size:
            size = size.split(':')[1]
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
        if licenses_node < self.max_node_num():
            LOG.error('node节点数量超标')
        else:
            return True
        return False

    def check_size(self):
        total_size = self.license_cluster_size
        if int(total_size) < int(self.max_cluster_size()):
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
        return lic.data
