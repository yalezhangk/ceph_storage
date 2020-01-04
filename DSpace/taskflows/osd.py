#!/usr/bin/env python
# -*- coding: utf-8 -*-
from oslo_log import log as logging
from taskflow.patterns import linear_flow as lf

from DSpace import exception
from DSpace import objects
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.base import Task
from DSpace.taskflows.base import Taskflow
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


class OsdCreateTaskflow(Taskflow):
    def taskflow(self, **kwargs):
        wf = lf.Flow('OsdCreateTaskflow')
        wf.add(OsdConfigSet())
        wf.add(OsdDiskPrepare())
        wf.add(OsdActive())
        wf.add(OsdWaitUp())
        return wf
