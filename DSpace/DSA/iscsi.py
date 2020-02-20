#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

from DSpace import exception as exc
from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools import iscsi

logger = logging.getLogger(__name__)


class IscsiHandler(AgentBaseHandler):
    def __init__(self, *args, **kwargs):
        super(IscsiHandler, self).__init__(*args, **kwargs)
        container_name = "athena_tcmu_runner"

        # try restore iscsi target config
        retry_interval = 5
        retry_times = 0
        while retry_times < 3:
            try:
                status = self.docker_servcie_status(self.ctxt, container_name)
                logger.debug("tcmu container status: %s", status)
                if status == "running" and self.node.role_block_gateway:
                    logger.info("trying to restore iscsi config")
                    iscsi.restore_target()
                    break
                else:
                    logger.warning("tcmu is not running, try again")
                    retry_times += 1
                    time.sleep(retry_interval)
                    retry_interval *= 2
            except Exception as e:
                logger.exception("restore iscsi config error: %s", e)

    def mount_bgw(self, context, access_path, node):
        logger.debug("will create iscsi target for access_path: %s",
                     access_path.name)
        iqn_target = access_path.iqn
        iscsi.create_target(iqn_target)
        iscsi.create_portal(iqn_target)
        iscsi.enable_tpg(iqn_target, True)

    def unmount_bgw(self, context, access_path):
        logger.debug("will delete iscsi target for access_path: %s",
                     access_path.name)
        iqn_target = access_path.iqn
        iscsi.delete_target(iqn_target)

    def _update_chap(self, iqn_target, chap_enable, username, password):
        if chap_enable:
            iscsi.chap_enable(iqn_target, username, password)
        else:
            iscsi.chap_disable(iqn_target)

    def bgw_set_chap(self, context, node, access_path, chap_enable,
                     username, password):
        logger.debug("iscsi target set chap, enable: {}, username: {}"
                     ", password: {}".format(chap_enable, username, password))
        iqn_target = access_path.iqn
        self._update_chap(iqn_target, chap_enable, username, password)

    def bgw_create_mapping(self, context, node, access_path,
                           volume_client, volumes):
        iqn_target = access_path.iqn
        iqn_initiator = volume_client.iqn
        logger.debug("iscsi target create mapping target: %s, acl: %s, "
                     "volumes: %s", access_path.iqn, volume_client.iqn,
                     volumes)
        iscsi.create_acl(iqn_target, iqn_initiator)
        for vol in volumes:
            self._create_acl_mapped_lun(iqn_target, iqn_initiator, vol)
        self._update_chap(iqn_target, access_path.chap_enable,
                          access_path.chap_username,
                          access_path.chap_password)

    def bgw_remove_mapping(self, context, node, access_path,
                           volume_client, volumes):
        iqn_target = access_path.iqn
        iqn_initiator = volume_client.iqn
        logger.debug("iscsi target remove mapping target: %s, acl: %s, "
                     "volumes: %s", access_path.iqn, volume_client.iqn,
                     volumes)
        iscsi.delete_acl(iqn_target, iqn_initiator)
        for vol in volumes:
            iscsi.delete_user_backstore(vol.volume_name)

    def _create_acl_mapped_lun(self, iqn_target, iqn_initiator, volume):
        so = None
        pool = volume.pool.pool_name
        so = iscsi.get_user_backstore(volume.volume_name)
        if not so:
            logger.info("trying to create a new user backstore: %s/%s",
                        pool, volume.volume_name)
            so = iscsi.create_user_backstore(pool,
                                             volume.volume_name,
                                             volume.size)
        lun = iscsi.get_lun_id(iqn_target, volume.volume_name)
        if lun is None:
            lun = iscsi.create_lun(iqn_target, so)
            logger.info("trying to create a new lun for volume: %s",
                        volume.volume_name)
        else:
            logger.info("using a exists lun<%s>(%s) to create mapped lun",
                        lun, volume.volume_name)
        iscsi.create_mapped_lun(iqn_target, iqn_initiator, lun)

    def bgw_add_volume(self, context, node, access_path,
                       volume_client, volumes):
        iqn_target = access_path.iqn
        iqn_initiator = volume_client.iqn
        logger.debug("iscsi target remove mapping target: %s, acl: %s, "
                     "volumes: %s", access_path.iqn, volume_client.iqn,
                     volumes)
        if not iscsi.get_acl(iqn_target, iqn_initiator):
            raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)
        for vol in volumes:
            self._create_acl_mapped_lun(iqn_target, iqn_initiator, vol)

    def bgw_remove_volume(self, context, node, access_path,
                          volume_client, volumes):
        iqn_target = access_path.iqn
        iqn_initiator = volume_client.iqn
        logger.debug("iscsi target remove volume, target: %s, acl: %s, "
                     "volumes: %s", access_path.iqn, volume_client.iqn,
                     volumes)
        if not iscsi.get_acl(iqn_target, iqn_initiator):
            raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)

        for vol in volumes:
            iscsi.remove_acl_mapped_lun(iqn_target, iqn_initiator,
                                        vol.volume_name)

    def bgw_change_client_group(self, context, access_path, volumes,
                                volume_clients, new_volume_clients):
        iqn_target = access_path.iqn
        for volume_client in volume_clients:
            iqn_initiator = volume_client['iqn']
            iscsi.delete_acl(iqn_target, iqn_initiator)
            logger.debug("iscsi target %s, delete acl %s",
                         iqn_target, iqn_initiator)
        for volume_client in new_volume_clients:
            iqn_initiator = volume_client['iqn']
            iscsi.create_acl(iqn_target, iqn_initiator)
            logger.debug("iscsi target %s, create acl %s",
                         iqn_target, iqn_initiator)
            for vol in volumes:
                self._create_acl_mapped_lun(iqn_target, iqn_initiator, vol)
        self._update_chap(iqn_target, access_path.chap_enable,
                          access_path.chap_username, access_path.chap_password)

    def bgw_set_mutual_chap(self, ctxt, access_path, volume_clients,
                            mutual_chap_enable, mutual_username,
                            mutual_password):
        iqn_target = access_path.iqn
        for volume_client in volume_clients:
            iqn_initiator = volume_client.iqn
            if mutual_chap_enable:
                iscsi.set_acl_mutual_chap(
                    iqn_target, iqn_initiator, mutual_username,
                    mutual_password)
            else:
                iscsi.set_acl_mutual_chap(iqn_target, iqn_initiator, "", "")

    def bgw_clear_all(self, ctxt):
        logger.info("clear all block gateway configs")
        iscsi.clear_all()
