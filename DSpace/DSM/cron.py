import logging
import time
from itertools import groupby
from operator import itemgetter

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSA.client import AgentClientManager
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class CronHandler(AdminBaseHandler):
    def __init__(self, *args, **kwargs):
        super(CronHandler, self).__init__(*args, **kwargs)
        self.ctxt = RequestContext(user_id="admin", project_id="stor",
                                   is_admin=True)
        self.clusters = objects.ClusterList.get_all(self.ctxt)
        self.task_submit(self._osd_slow_requests_set_all)

    def _osd_slow_requests_set(self, ctxt):
        while True:
            # 分组
            osds = objects.OsdList.get_all(ctxt, expected_attrs=['node'])
            osds.sort(key=itemgetter('node_id'))
            osds = groupby(osds, itemgetter('node_id'))
            osd_all = dict([(key, list(group)) for key, group in osds])
            res = {"slow_request_total": 0,
                   "slow_request_sum": [],
                   'slow_request_ops': []}
            total = 0
            # 循环 发送osd列表至每个agent
            for node_id, osds in osd_all.items():
                client = AgentClientManager(
                    ctxt, cluster_id=ctxt.cluster_id
                ).get_client(node_id=node_id)
                try:
                    data = client.ceph_slow_request(ctxt, osds)
                    # 循环每个agent返回的osd的慢请求列表
                    for i in range(len(data)):
                        # 将每个osd里面的数据进行处理
                        res['slow_request_ops'].extend([{
                            "id": data[i]['id'],
                            "osd_id": data[i]['osd_id'],
                            "node_id": data[i]['node_id'],
                            "hostname": data[i]['hostname'],
                            "type": sr['description'].split('(', 1)[0],
                            "duration": sr['duration'],
                        } for sr in data[i]["ops"]])
                        # 慢请求汇总
                        data[i]['total'] = len(data[i]["ops"])
                        total += len(data[i]["ops"])
                        data[i].pop("ops")
                except exception.StorException as e:
                    logger.exception("osd slow requests set error: %s", e)
                res['slow_request_sum'].extend(data)
            # 集群慢请求汇总
            res['slow_request_total'] = total
            # 排序
            res["slow_request_sum"].sort(key=itemgetter('total'), reverse=True)
            res["slow_request_ops"].sort(key=itemgetter('duration'),
                                         reverse=True)
            self.slow_requests.update({ctxt.cluster_id: res})
            time.sleep(CONF.slow_request_get_time_interval)

    def _osd_slow_requests_set_all(self):
        for cluster in self.clusters:
            ctxt = RequestContext(user_id='admin',
                                  is_admin=True,
                                  cluster_id=cluster.id)
            logger.info("Get cluster %s osds slow request "
                        "from agent" % cluster.id)
            self._osd_slow_requests_set(ctxt)
