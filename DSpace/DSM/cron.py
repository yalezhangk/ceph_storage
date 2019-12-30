import logging
import time
from itertools import groupby
from operator import itemgetter

import six
from tooz.coordination import LockAcquireFailed

from DSpace import context as context_tool
from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
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
        self.ctxt = context_tool.get_context()
        self.clusters = objects.ClusterList.get_all(self.ctxt)
        self.task_submit(self._check_ceph_cluster_status)
        self.task_submit(self._osd_slow_requests_get_all)
        self.task_submit(self._osd_tree_cron)
        self.task_submit(self._dsa_check_cron)
        self.task_submit(self._node_check_cron)

    def _osd_slow_requests_get(self):
        for cluster in self.clusters:
            ctxt = context_tool.get_context(cluster_id=cluster.id)
            logger.info("Get cluster %s osds slow request "
                        "from agent" % cluster.id)
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
                    res['slow_request_sum'].extend(data)
                except exception.StorException as e:
                    logger.exception("osd slow requests get error: %s", e)
            # 集群慢请求汇总
            res['slow_request_total'] = total
            # 排序
            res["slow_request_sum"].sort(key=itemgetter('total'), reverse=True)
            res["slow_request_ops"].sort(key=itemgetter('duration'),
                                         reverse=True)
            self.slow_requests.update({ctxt.cluster_id: res})

    def _osd_slow_requests_get_all(self):
        logger.debug("Start ceph cluster check crontab")
        while True:
            try:
                self._osd_slow_requests_get()
            except Exception as e:
                logger.exception("Osd slow request set Exception: %s", e)
            time.sleep(CONF.slow_request_get_time_interval)

    def _osd_tree_cron(self):
        logger.debug("Start osd check crontab")
        if not CONF.osd_heartbeat_check or not CONF.heartbeat_check:
            logger.info("osd check not enable")
            return
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
        self.send_websocket(context, osd, "OSD_RESTART", msg)
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

    def get_osd_status(self, context, osd_id, ceph_client):
        osd_nodes = self._get_osd_tree(ceph_client)
        osd_status = {}
        for osd_node in osd_nodes:
            if osd_node.get("id") == osd_id:
                osd_status = osd_node
                break
            else:
                continue
        if not osd_status:
            return ""
        status = ""
        # get in or out
        if osd_status.get('reweight'):
            status += "in&"
        else:
            status += "out&"
        # get up or down
        serice_status = self._check_ceph_osd_status(context, ceph_client)
        if serice_status:
            up_down = serice_status.get("osd.{}".format(osd_status.get('id')))
        else:
            up_down = osd_status.get('status')
        if not up_down:
            return ""
        status += up_down
        return status

    def _set_osd_status(self, context, osd_status, ceph_client):
        osd_id = osd_status.get('id')
        if osd_id < 0:
            return
        osd = objects.OsdList.get_all(context, filters={"osd_id": osd_id})
        if not osd:
            return
        osd = osd[0]
        if osd.status in [s_fields.OsdStatus.DELETING,
                          s_fields.OsdStatus.CREATING,
                          s_fields.OsdStatus.REPLACE_PREPARED,
                          s_fields.OsdStatus.REPLACE_PREPARING,
                          s_fields.OsdStatus.PROCESSING]:
            return
        status = self.get_osd_status(context, osd_id, ceph_client)
        if not status:
            return
        node = objects.Node.get_by_id(context, osd.node_id)
        if not self.if_service_alert(context, node=node):
            return
        try:
            if status == "in&up":
                err_status = [s_fields.OsdStatus.OFFLINE,
                              s_fields.OsdStatus.RESTARTING,
                              s_fields.OsdStatus.ERROR]
                if osd.status in err_status:
                    msg = _("osd.{} is active").format(osd.osd_id)
                    self.send_service_alert(
                        context, osd, "osd_status", "Osd", "INFO", msg,
                        "OSD_ACTIVE")
                osd.conditional_update({
                    "status": s_fields.OsdStatus.ACTIVE
                }, expected_values={
                    "status": err_status
                })
            elif status == "in&down":
                if osd.status in [s_fields.OsdStatus.ACTIVE]:
                    logger.warning("osd.%s status is %s", osd.osd_id, status)
                    osd.conditional_update({
                        "status": s_fields.OsdStatus.OFFLINE
                    }, expected_values={
                        "status": s_fields.OsdStatus.ACTIVE
                    })
                    msg = _("osd.{} is offline").format(osd.osd_id)
                    self.send_service_alert(
                        context, osd, "osd_status", "Osd", "WARN", msg,
                        "OSD_OFFLINE")
                    if self.debug_mode:
                        return
                    osd.conditional_update({
                        "status": s_fields.OsdStatus.RESTARTING
                    }, expected_values={
                        "status": s_fields.OsdStatus.ACTIVE
                    })
                    self.task_submit(self._restart_osd, context, osd)
                    return
            elif status == "out&up" or status == "out&down":
                if osd.status in [s_fields.OsdStatus.OFFLINE,
                                  s_fields.OsdStatus.RESTARTING,
                                  s_fields.OsdStatus.ERROR]:
                    return
                logger.warning("osd.%s status is %s", osd.osd_id, status)
                msg = _("osd.{} is offline").format(osd.osd_id)
                self.send_service_alert(context, osd, "osd_status", "Osd",
                                        "WARN", msg, "OSD_OFFLINE")
                osd.status = s_fields.OsdStatus.OFFLINE
                osd.save()
        except exception.OsdNotFound as e:
            logger.warning(e)

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

    def _make_osd_list(self, osds):
        res = {}
        for osd in osds:
            res.update({osd.osd_id: osd})
        return res

    def _get_osd_tree(self, ceph_client):
        osd_tree = ceph_client.get_osd_tree()
        nodes = osd_tree.get("nodes") + osd_tree.get("stray")
        return nodes

    # @synchronized('cron_osd_tree', blocking=False)
    def osd_check(self):
        for cluster in self.clusters:
            context = context_tool.get_context(cluster_id=cluster.id)
            if not self.get_ceph_cluster_status(context):
                continue
            ceph_client = CephTask(context)
            osds = objects.OsdList.get_all(context)
            if not osds:
                logger.debug("Cluster %s has no osd, ignore", cluster.id)
                continue
            osds = self._make_osd_list(osds)
            nodes = self._get_osd_tree(ceph_client)
            for osd_status in nodes:
                if osd_status.get('id') < 0:
                    continue
                self._set_osd_status(context, osd_status, ceph_client)
                if osds.get(str(osd_status.get('id'))):
                    osds.pop(str(osd_status.get('id')))
            if osds:
                for osd_id, osd in six.iteritems(osds):
                    logger.error("Osd.%s is not in cluster", osd_id)
                    osd.status = s_fields.OsdStatus.ERROR
                    osd.save()

    def _dsa_check_cron(self):
        logger.info("Start dsa check crontab")
        if not CONF.dsa_heartbeat_check or not CONF.heartbeat_check:
            logger.info("dsa check not enable")
            return
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
        dsa.status = s_fields.ServiceStatus.STARTING
        dsa.save()
        msg = _("Node {}: DSA service status is inactive, trying to restart.")\
            .format(node.hostname)
        self.send_websocket(ctxt, dsa, "SERVICE_RESTART", msg)
        while retry_times < 10:
            try:
                docker_tool.restart(container_name)
                if docker_tool.status(container_name):
                    logger.info("DSA service has been restarted")
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
            ctxt = context_tool.get_context(cluster_id=dsa.cluster_id)
            dsa_status = dsa.status
            self.check_service_status(self.ctxt, dsa)
            node = objects.Node.get_by_id(ctxt, dsa.node_id)
            if (dsa.status == s_fields.ServiceStatus.INACTIVE and
                    dsa_status == s_fields.ServiceStatus.ACTIVE):
                logger.error("DSA in node %s is inactive", dsa.node_id)
                if not self.if_service_alert(ctxt, node=node):
                    continue
                msg = _("Node {}: DSA status is inactive"
                        ).format(node.hostname)
                self.send_service_alert(
                    ctxt, dsa, "service_status", "DSA", "WARN", msg,
                    "SERVICE_INACTIVE")
                if self.debug_mode:
                    continue
                dsa.conditional_update({
                    "status": s_fields.ServiceStatus.STARTING
                }, expected_values={
                    "status": s_fields.ServiceStatus.ACTIVE
                })
                self.task_submit(self._restart_dsa, ctxt, dsa, node)
            if (dsa.status == s_fields.ServiceStatus.ACTIVE and
                    (dsa_status in [s_fields.ServiceStatus.INACTIVE,
                                    s_fields.ServiceStatus.STARTING])):
                logger.error("DSA in node %s is active", dsa.node_id)
                if not self.if_service_alert(ctxt, node=node):
                    continue
                msg = _("Node {}: DSA status is active"
                        ).format(node.hostname)
                self.send_service_alert(
                    ctxt, dsa, "service_status", "DSA", "INFO", msg,
                    "SERVICE_ACTIVE")

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
            ctxt = context_tool.get_context(cluster_id=node.cluster_id)
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
                    node.conditional_update({
                        "status": s_fields.NodeStatus.WARNING
                    }, expected_values={
                        "status": s_fields.NodeStatus.ACTIVE
                    })
            else:
                if node.status == s_fields.NodeStatus.ACTIVE:
                    node.conditional_update({
                        "status": s_fields.NodeStatus.ACTIVE
                    }, expected_values={
                        "status": s_fields.NodeStatus.WARNING
                    })

    def _check_ceph_cluster_status(self):
        logger.debug("Start ceph cluster check crontab")
        while True:
            try:
                self._ceph_status_check()
            except LockAcquireFailed as e:
                logger.debug(e)
                time.sleep(CONF.ceph_mon_check_interval * 2)
                continue
            except Exception as e:
                logger.exception("Ceph cluster check cron Exception: %s", e)
            time.sleep(CONF.ceph_mon_check_interval)

    def _ceph_check_retry(self, ceph_client, cluster):
        # check ceph status
        ceph_client.ceph_status_check()
        cluster.ceph_status = True
        cluster.save()

    @synchronized(lock_name="ceph_status_check", blocking=False)
    def _ceph_status_check(self):
        self.clusters = objects.ClusterList.get_all(self.ctxt)

        for cluster in self.clusters:
            context = context_tool.get_context(cluster_id=cluster.id)
            # check monitor role
            nodes = objects.NodeList.get_all(
                context, filters={"role_monitor": True})
            if not nodes:
                cluster.ceph_status = False
                cluster.save()
                logger.info("no monitor found")
                continue
            # check ceph config
            mon_host = objects.ceph_config.ceph_config_get(
                context, "global", "mon_host")
            if not mon_host:
                cluster.ceph_status = False
                cluster.save()
                logger.info("no mon host found")
                continue

            ceph_client = CephTask(context)
            status = cluster.ceph_status
            try:
                self._ceph_check_retry(ceph_client, cluster)
                if not status:
                    msg = _("reconnect to ceph cluster {}"
                            ).format(cluster.display_name)
                    self.send_service_alert(
                        context, cluster, "service_status", "MON", "INFO",
                        msg, "SERVICE_ACTIVE")
            except exception.StorException as e:
                if not self.if_service_alert(context):
                    continue
                logger.warning("Could not connect to ceph cluster %s: %s",
                               cluster.display_name, e)
                cluster.ceph_status = False
                cluster.save()
                if status:
                    msg = _("Could not connect to ceph cluster {}"
                            ).format(cluster.display_name)
                    self.send_service_alert(
                        context, cluster, "service_status", "MON", "ERROR",
                        msg, "SERVICE_ACTIVE")
