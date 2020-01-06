#!/usr/bin/env python
# -*- coding: utf-8 -*-
from oslo_log import log as logging
from taskflow.patterns import linear_flow as lf

from DSpace import exception
from DSpace import objects
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.base import Task
from DSpace.taskflows.base import Taskflow
from DSpace.taskflows.base import TaskflowRegistry
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeAgentMixin
from DSpace.utils import retry
from DSpace.utils.cluster_config import CEPH_CONFIG_DIR
from DSpace.utils.cluster_config import CEPH_LIB_DIR

logger = logging.getLogger(__name__)


class OsdConfigSet(Task, NodeAgentMixin):
    """Osd config set

    Update db and ceph config
    """
    def _get_configs(self, ctxt, osd):
        configs = {
            "global": objects.ceph_config.ceph_config_group_get(
                ctxt, "global"),
            osd.osd_name: objects.ceph_config.ceph_config_group_get(
                ctxt, osd.osd_name),
        }
        return configs

    def _update_db(self, ctxt, osd):
        # update db
        logger.info("%s update db", osd.osd_name)
        if osd.cache_partition_id:
            ceph_cfg = objects.CephConfig(
                ctxt, group="osd.%s" % osd.osd_id,
                key='backend_type',
                value=s_fields.OsdBackendType.T2CE,
                value_type=s_fields.ConfigType.STRING
            )
            ceph_cfg.create()
        ceph_cfg = objects.CephConfig(
            ctxt, group="osd.%s" % osd.osd_id,
            key='osd_objectstore',
            value=osd.type,
            value_type=s_fields.ConfigType.STRING
        )
        ceph_cfg.create()

    def execute(self, ctxt, osd, tf):
        logger.info("%s config task", osd.osd_name)
        self.prepare_task(ctxt, tf)
        # update db
        self._update_db(ctxt, osd)

        # update ceph config
        agent = self._get_agent(ctxt, osd.node)
        configs = self._get_configs(ctxt, osd)
        logger.info("%s set ceph config", osd.osd_name)
        agent.ceph_config_set(ctxt, configs)
        enable_cephx = objects.sysconfig.sys_config_get(
            ctxt, key=ConfigKey.ENABLE_CEPHX
        )
        if enable_cephx:
            logger.debug("cephx is enable")
            self.init_admin_key(ctxt, osd.node)
            self.init_bootstrap_keys(ctxt, "osd", osd.node)
        else:
            logger.debug("cephx is not enable")

        self.finish_task()

    def init_admin_key(self, ctxt, node):
        agent = self._get_agent(ctxt, node)
        admin_entity = "client.admin"
        keyring_name = "ceph.client.admin.keyring"
        admin_keyring = objects.CephConfig.get_by_key(
            ctxt, 'keyring', 'client.admin')
        if not admin_keyring:
            logger.error("cephx is enable, but no admin keyring found")
            raise exception.CephException(
                message='cephx is enable, but no admin'
                        'keyring found')
        agent.ceph_key_write(ctxt, admin_entity, CEPH_CONFIG_DIR,
                             keyring_name, admin_keyring.value)

    def init_bootstrap_keys(self, ctxt, bootstrap_type, node):
        agent = self._get_agent(ctxt, node)
        bootstrap_entity = "client.bootstrap-{}".format(bootstrap_type)
        keyring_dir = "{}/bootstrap-{}".format(CEPH_LIB_DIR, bootstrap_type)
        keying_name = 'ceph.keyring'
        ceph_client = CephTask(ctxt)
        auth = ceph_client.auth_get_key(bootstrap_entity)
        bootstrap_key = auth.get("key")
        if not bootstrap_key:
            logger.error("cephx is enable, but no bootstrap keyring found")
            raise exception.CephException(
                message='cephx is enable, but no bootstrap'
                        'keyring found')
        agent.ceph_key_write(ctxt, bootstrap_entity, keyring_dir,
                             keying_name, bootstrap_key)


class OsdDiskPrepare(Task, NodeAgentMixin):
    def execute(self, ctxt, osd, tf):
        logger.info("%s disk prepare task", osd.osd_name)
        self.prepare_task(ctxt, tf)
        agent = self._get_agent(ctxt, osd.node)
        agent.ceph_prepare_disk(ctxt, osd)
        self.finish_task()


class OsdActive(Task, NodeAgentMixin):
    def execute(self, ctxt, osd, tf):
        logger.info("%s active task", osd.osd_name)
        self.prepare_task(ctxt, tf)
        agent = self._get_agent(ctxt, osd.node)
        agent.ceph_active_disk(ctxt, osd)
        self.finish_task()


class OsdWaitUp(Task):
    """Osd wait UP

    Wait osd up and sync osd info
    """
    @retry(exception.OsdStatusNotUp, retries=7)
    def wait_osd_up(self, ctxt, osd_name):
        # retries=6: max wait 127s
        logger.info("check osd is up: %s", osd_name)
        ceph_client = CephTask(ctxt)
        osd_tree = ceph_client.get_osd_tree()
        _osds = osd_tree.get("stray") or []
        osds = filter(lambda x: (x.get('name') == osd_name and
                                 x.get('status') == "up"),
                      _osds)
        if len(list(osds)) > 0:
            logger.info("osd is up: %s", osd_name)
            return True
        else:
            logger.info("osd not up: %s", osd_name)
            raise exception.OsdStatusNotUp()

    def update_size(self, ctxt,  osd):
        logger.info("get osd size: %s", osd.osd_name)
        ceph_client = CephTask(ctxt)
        metadata = ceph_client.osd_metadata(osd.osd_id)
        size = metadata.get("bluestore_bdev_size")
        if size:
            osd.size = size
            osd.save()
            logger.info("get osd size: %s %s", osd.osd_name, size)

    def execute(self, ctxt, osd, tf):
        logger.info("%s wait up task", osd.osd_name)
        self.prepare_task(ctxt, tf)
        self.wait_osd_up(ctxt, osd.osd_name)
        self.update_size(ctxt, osd)
        self.finish_task()


class OsdTaskflowMixin(object):
    def _format_args(self, osd=None):
        return {"osd_id": osd.id}

    def _mark_osd_error(self, tf, expected_status=None):
        if 'osd_id' not in tf.args:
            raise exception.TaskflowArgsError(taskflow_id=tf.id, args=tf.args)
        osd_id = tf.args['osd_id']
        osd = objects.Osd.get_by_id(self.ctxt, osd_id)
        if expected_status and osd.status != expected_status:
            logger.warning("osd(db_id=%s) is not creating", osd.id)
        else:
            osd.status = s_fields.OsdStatus.ERROR
            osd.save()
        err_msg = _("DSpace manager service stoped.")
        begin_action = objects.ActionLog.get_by_id(self.ctxt, tf.action_log_id)
        begin_action.finish_action(
            osd.id, osd.osd_name,
            osd, osd.status, err_msg=err_msg)


@TaskflowRegistry.register
class OsdCreateTaskflow(Taskflow, OsdTaskflowMixin):
    def taskflow(self, osd=None):
        wf = lf.Flow('OsdCreateTaskflow')
        wf.add(OsdConfigSet())
        wf.add(OsdDiskPrepare())
        wf.add(OsdActive())
        wf.add(OsdWaitUp())
        return wf

    def format_args(self, osd=None):
        return self._format_args(osd)

    def failed(self, **kwargs):
        logger.info("%s called", self.name)
        self._mark_osd_error(self.tf, s_fields.OsdStatus.CREATING)
        # call parent method, mark taskflow failed
        super(OsdCreateTaskflow, self).failed(**kwargs)


class OsdMarkOut(Task):
    def execute(self, ctxt, osd, tf):
        logger.info("%s osd mark out", osd.osd_name)
        self.prepare_task(ctxt, tf)
        ceph_client = CephTask(ctxt)
        logger.info("out osd %s from cluster", osd.osd_name)
        ceph_client.mark_osds_out(osd.osd_name)
        self.finish_task()


class OsdClearData(Task, NodeAgentMixin):
    def execute(self, ctxt, osd, tf):
        logger.info("%s osd mark out", osd.osd_name)
        self.prepare_task(ctxt, tf)
        agent = self._get_agent(ctxt, osd.node)
        osd = agent.ceph_osd_destroy(ctxt, osd)
        self.finish_task()


class OsdRemoveFromCluster(Task):
    def execute(self, ctxt, osd, tf):
        logger.info("%s osd mark out", osd.osd_name)
        self.prepare_task(ctxt, tf)
        ceph_client = CephTask(ctxt)
        logger.info("remove osd %s from cluster", osd.osd_name)
        ceph_client.osd_remove_from_cluster(osd.osd_name)
        self.finish_task()


class OsdClearDB(Task):
    def _config_remove(self, ctxt, osd):
        logger.debug("osd clear config")
        osd_cfgs = objects.CephConfigList.get_all(
            ctxt, filters={'group': "osd.%s" % osd.osd_id}
        )
        for cfg in osd_cfgs:
            cfg.destroy()

    def _clear_partition_role(self, ctxt, osd):
        logger.debug("osd clear partition role")
        osd.disk.status = s_fields.DiskStatus.AVAILABLE
        osd.disk.save()

        accelerate_disk = []
        if osd.db_partition_id:
            osd.db_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.db_partition.save()
            accelerate_disk.append(osd.db_partition.disk_id)
        if osd.cache_partition_id:
            osd.cache_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.cache_partition.save()
            accelerate_disk.append(osd.cache_partition.disk_id)
        if osd.journal_partition_id:
            osd.journal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.journal_partition.save()
            accelerate_disk.append(osd.journal_partition.disk_id)
        if osd.wal_partition_id:
            osd.wal_partition.status = s_fields.DiskStatus.AVAILABLE
            osd.wal_partition.save()
            accelerate_disk.append(osd.wal_partition.disk_id)
        accelerate_disk = set(accelerate_disk)
        if accelerate_disk:
            ac_disk = objects.DiskList.get_all(
                ctxt, filters={'id': accelerate_disk})
            for disk in ac_disk:
                disk.status = s_fields.DiskStatus.AVAILABLE
                disk.save()

    def execute(self, ctxt, osd, tf):
        logger.info("%s osd clear db", osd.osd_name)
        self.prepare_task(ctxt, tf)
        self._config_remove(ctxt, osd)
        self._clear_partition_role(ctxt, osd)
        self.finish_task()


@TaskflowRegistry.register
class OsdDeleteTaskflow(Taskflow, OsdTaskflowMixin):
    """Osd delete

    mark osd out
    deactive and clean disk
    remove osd from cluster
    clear db update
    """
    def taskflow(self, **kwargs):
        wf = lf.Flow('OsdDeleteTaskflow')
        wf.add(OsdMarkOut())
        wf.add(OsdClearData())
        wf.add(OsdRemoveFromCluster())
        wf.add(OsdClearDB())
        return wf

    def format_args(self, osd=None):
        return self._format_args(osd)

    def failed(self, **kwargs):
        logger.info("%s called", self.name)
        self._mark_osd_error(self.tf, s_fields.OsdStatus.DELETING)
        # call parent method, mark taskflow failed
        super(OsdDeleteTaskflow, self).failed(**kwargs)
