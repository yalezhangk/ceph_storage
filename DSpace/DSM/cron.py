import logging
import time
from itertools import groupby
from operator import itemgetter

from tooz.coordination import LockAcquireFailed

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSA.client import AgentClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool
from DSpace.tools.docker import Docker as DockerTool
from DSpace.utils.coordination import synchronized

logger = logging.getLogger(__name__)


class CronHandler(AdminBaseHandler):
    def __init__(self, *args, **kwargs):
        super(CronHandler, self).__init__(*args, **kwargs)
        self.ctxt = RequestContext(user_id="admin", project_id="stor",
                                   is_admin=False)
        self.clusters = objects.ClusterList.get_all(self.ctxt)
        self.task_submit(self._osd_slow_requests_set_all)
        self.task_submit(self._osd_tree_cron)
        self.task_submit(self._dsa_check_cron)
        self.task_submit(self._node_check_cron)

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

    def _osd_tree_cron(self):
        logger.debug("Start osd check crontab")
        while True:
            try:
                self.osd_check()
            except LockAcquireFailed as e:
                logger.debug(e)
                time.sleep(CONF.osd_check_interval * 2)
                continue
            except Exception as e:
                logger.exception("Osd check cron exception: %s", e)
            time.sleep(CONF.osd_check_interval)

    def _restart_osd(self, context, osd):
        logger.info("osd.%s is down, try to restart", osd.osd_id)
        msg = _("osd.{} service status is inactive, trying to restart").format(
            osd.osd_id)
        self.send_service_alert(context, osd, "osd_status", "Osd", "INFO",
                                msg, "OSD_RESTART")
        node = objects.Node.get_by_id(context, osd.node_id)
        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        ceph_tool = CephTool(ssh)
        try:
            ceph_tool.systemctl_restart('osd', osd.osd_id)
        except exception.StorException as e:
            logger.error(e)
            osd.status = s_fields.OsdStatus.OFFLINE
            osd.save()
            msg = _("osd.{} cannot be restarted, mark to offline").format(
                osd.osd_id)
            self.send_service_alert(context, osd, "osd_status", "Osd",
                                    "ERROR", msg, "OSD_OFFLINE")

    def _set_osd_status(self, context, osd_status, up_down):
        osd_id = osd_status.get('id')
        if osd_id < 0:
            return
        status = ""
        if osd_status.get('reweight'):
            status += "in&"
        else:
            status += "out&"
        # append up or down
        if not up_down:
            return
        status += up_down
        osd = objects.Osd.get_by_osd_id(context, osd_id)
        if osd.status in [s_fields.OsdStatus.DELETING,
                          s_fields.OsdStatus.CREATING,
                          s_fields.OsdStatus.REPLACE_PREPARED,
                          s_fields.OsdStatus.REPLACE_PREPARING,
                          s_fields.OsdStatus.RESTARTING,
                          s_fields.OsdStatus.PROCESSING]:
            return
        if status == "in&up":
            osd.status = s_fields.OsdStatus.ACTIVE
            osd.save()
        elif status == "in&down":
            if osd.status in [s_fields.OsdStatus.ACTIVE]:
                if self.debug_mode:
                    osd.status = s_fields.OsdStatus.OFFLINE
                    osd.save()
                    msg = _("osd.{} is offline").format(osd.osd_id)
                    self.send_service_alert(context, osd, "osd_status", "Osd",
                                            "WARN", msg, "OSD_OFFLINE")
                    return
                osd.status = s_fields.OsdStatus.RESTARTING
                osd.save()
                self.task_submit(self._restart_osd, context, osd)
                return
        elif status == "out&up":
            if osd.status in [s_fields.OsdStatus.OFFLINE]:
                return
            msg = _("osd.{} is offline").format(osd.osd_id)
            self.send_service_alert(context, osd, "osd_status", "Osd",
                                    "WARN", msg, "OSD_OFFLINE")
            osd.status = s_fields.OsdStatus.OFFLINE
            osd.save()
        elif status == "out&down":
            if osd.status in [s_fields.OsdStatus.OFFLINE,
                              s_fields.OsdStatus.ERROR]:
                return
            msg = _("osd.{} is offline").format(osd.osd_id)
            self.send_service_alert(context, osd, "osd_status", "Osd",
                                    "WARN", msg, "OSD_OFFLINE")
            osd.status = s_fields.OsdStatus.OFFLINE
            osd.save()

    def _get_osd_status_from_dsa(self, context):
        osds = objects.OsdList.get_all(
            context, expected_attrs=['node'])
        osds.sort(key=itemgetter('node_id'))
        osds = groupby(osds, itemgetter('node_id'))
        osd_all = dict([(key, list(group)) for key, group in osds])
        osds_status = {}
        for node_id, osds in osd_all.items():
            client = AgentClientManager(
                context, cluster_id=context.cluster_id
            ).get_client(node_id=node_id)
            try:
                res = client.get_osds_status(context, osds)
                osds_status.update(res)
            except exception.StorException as e:
                logger.error(e)
                continue
        return osds_status

    def _check_ceph_osd_status(self, context, ceph_client):
        min_up_ratio = objects.CephConfig.get_by_key(
            context, group="*", key="mon_osd_min_up_ratio")
        if not min_up_ratio:
            min_up_ratio = 0.3
        osd_stat = ceph_client.get_osd_stat()
        num_osds = osd_stat.get("num_osds")
        if not num_osds:
            return {}
        up_ratio = osd_stat.get("num_up_osds") / num_osds
        if up_ratio <= min_up_ratio:
            return self._get_osd_status_from_dsa(context)
        return {}

    @synchronized('cron_osd_tree', blocking=False)
    def osd_check(self):
        for cluster in self.clusters:
            context = RequestContext(user_id="admin", project_id="stor",
                                     is_admin=False, cluster_id=cluster.id)
            ceph_client = CephTask(context)
            osds = objects.OsdList.get_count(context)
            if not osds:
                logger.debug("Cluster %s has no osd, ignore", cluster.id)
                continue
            status = self._check_ceph_osd_status(context, ceph_client)
            osd_tree = ceph_client.get_osd_tree()
            nodes = osd_tree.get("nodes")
            for osd_status in nodes:
                if osd_status.get('id') < 0:
                    continue
                if status:
                    up_down = status.get("osd.{}".format(osd_status.get('id')))
                else:
                    up_down = osd_status.get('status')
                self._set_osd_status(context, osd_status, up_down)
            stray = osd_tree.get("stray")
            for osd_status in stray:
                if status:
                    up_down = status.get("osd.{}".format(osd_status.get('id')))
                else:
                    up_down = osd_status.get('status')
                self._set_osd_status(context, osd_status, up_down)

    def _dsa_check_cron(self):
        logger.debug("Start dsa check crontab")
        while True:
            try:
                self.dsa_check()
            except LockAcquireFailed as e:
                logger.debug(e)
                time.sleep(CONF.dsa_check_interval * 2)
                continue
            except Exception as e:
                logger.exception("Dsa check cron Exception: %s", e)
            time.sleep(CONF.dsa_check_interval)

    def _restart_dsa(self, ctxt, dsa, node):
        logger.warning("DSA in node %s is down, trying to restart",
                       dsa.node_id)
        ssh = SSHExecutor(hostname=str(node.ip_address),
                          password=node.password)
        docker_tool = DockerTool(ssh)
        retry_times = 0
        container_name = self.map_util.base['DSA']
        msg = _("Node {}: DSA service status is inactive, trying to restart.")\
            .format(node.hostname)
        self.send_service_alert(
            ctxt, dsa, "service_status", dsa.name, "INFO",
            msg, "SERVICE_RESTART")
        while retry_times < 10:
            try:
                docker_tool.restart(container_name)
                if docker_tool.status(container_name):
                    break
            except exception.StorException as e:
                logger.error(e)
                retry_times += 1
                if retry_times == 10:
                    dsa.status = s_fields.ServiceStatus.ERROR
                    dsa.save()
                    msg = _(
                        "Node {}: DSA restart failed, mark it to error"
                    ).format(node.hostname)
                    self.send_service_alert(
                        ctxt, dsa, "service_status", "Service", "ERROR",
                        msg, "SERVICE_ERROR"
                    )

    @synchronized('cron_dsa_check', blocking=False)
    def dsa_check(self):
        dsas = objects.ServiceList.get_all(
            self.ctxt, filters={'name': 'DSA', 'cluster_id': '*'})
        logger.debug("dsa_check: %s", dsas)
        for dsa in dsas:
            ctxt = RequestContext(user_id='admin',
                                  is_admin=True,
                                  cluster_id=dsa.cluster_id)
            self.check_service_status(self.ctxt, dsa)
            logger.error("DSA in node %s is inactive", dsa.node_id)
            if dsa.status == s_fields.ServiceStatus.INACTIVE \
                    and not self.debug_mode:
                dsa.status = s_fields.ServiceStatus.STARTING
                dsa.save()
                node = objects.Node.get_by_id(ctxt, dsa.node_id)
                msg = _("Node {}: DSA in status is inactive"
                        ).format(node.hostname)
                self.send_service_alert(
                    ctxt, dsa, "service_status", "DSA", "WARN", msg,
                    "SERVICE_INACTIVE")
                self.task_submit(self._restart_dsa, ctxt, dsa, node)

    def _node_check_cron(self):
        logger.debug("Start node check crontab")
        while True:
            try:
                self.node_services_check()
            except LockAcquireFailed as e:
                logger.debug(e)
                time.sleep(CONF.dsa_check_interval * 2)
                continue
            except Exception as e:
                logger.exception("Dsa check cron Exception: %s", e)
            time.sleep(CONF.node_check_interval)

    @synchronized("cron_node_services_check", blocking=False)
    def node_services_check(self):
        nodes = objects.NodeList.get_all(
            self.ctxt, filters={'cluster_id': '*'})
        for node in nodes:
            ctxt = RequestContext(user_id='admin',
                                  is_admin=True,
                                  cluster_id=node.cluster_id)
            check_ok = True
            filters = {"node_id": node.id}
            services = objects.ServiceList.get_all(ctxt, filters=filters)
            for service in services:
                if service.status in [s_fields.ServiceStatus.INACTIVE,
                                      s_fields.ServiceStatus.ERROR]:
                    check_ok = False
                    break
            osds = objects.OsdList.get_all(ctxt, filters=filters)
            for osd in osds:
                if osd.status in [s_fields.OsdStatus.ERROR,
                                  s_fields.OsdStatus.WARNING,
                                  s_fields.OsdStatus.OFFLINE]:
                    check_ok = False
                    break
            rgws = objects.RadosgwList.get_all(ctxt, filters=filters)
            for rgw in rgws:
                if rgw.status in [s_fields.RadosgwStatus.INACTIVE,
                                  s_fields.RadosgwStatus.ERROR]:
                    check_ok = False
                    break
            if check_ok:
                if node.status == s_fields.NodeStatus.WARNING:
                    node.status = s_fields.NodeStatus.ACTIVE
                    node.save()
            else:
                if node.status == s_fields.NodeStatus.ACTIVE:
                    node.status = s_fields.NodeStatus.WARNING
                    node.save()
