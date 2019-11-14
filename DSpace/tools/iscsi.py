#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import filecmp
import json
import logging
import os
import shutil
from glob import glob

from rtslib_fb import LUN
from rtslib_fb import TPG
from rtslib_fb import FabricModule
from rtslib_fb import MappedLUN
from rtslib_fb import RTSRoot
from rtslib_fb import Target
from rtslib_fb import UserBackedStorageObject
from rtslib_fb.utils import RTSLibError
from rtslib_fb.utils import ignored

from DSpace import exception as exc

logger = logging.getLogger(__name__)

DEFAULT_SAVE_FILE = "/etc/target/saveconfig.json"

SIZE_SUFFIXES = ['M', 'G', 'T']


def convert_2_bytes(disk_size):
    try:
        # If it's already an integer or a string with no suffix then assume
        # it's already in bytes.
        return int(disk_size)
    except ValueError:
        pass

    power = [2, 3, 4]
    unit = disk_size[-1].upper()
    offset = SIZE_SUFFIXES.index(unit)
    # already validated, so no need for try/except clause
    value = int(disk_size[:-1])

    _bytes = value * (1024 ** power[offset])

    return _bytes


def current_targets():
    """
    Returns list of iqn's of currently defined iscsi targets.
    """
    existing_targets = []
    for target in FabricModule('iscsi').targets:
        existing_targets.append(
            target.wwn,
        )

    return existing_targets


def create_target(iqn):
    """
    Creates new iscsi target with given iqn, unless it already exists.
    """
    logger.info("trying to create target %s", iqn)
    if iqn in current_targets():
        logger.error('iscsi-target %s already exists', iqn)
        raise exc.IscsiTargetExists(iqn=iqn)
    else:
        try:
            Target(FabricModule("iscsi"), wwn=iqn)
            logger.debug("Creating iscsi-target %s", iqn)
        except RTSLibError as e:
            logger.error("Creating iscsi-target %s error: %s", iqn, e)
            raise exc.IscsiTargetError(action="create")
    logger.info("create target %s success", iqn)
    save_config()


def delete_target(iqn):
    """
    Recursively deletes all iscsi Target objects.
    """
    logger.info("trying to delete target %s", iqn)
    if not iqn:
        return None
    if iqn not in current_targets():
        # Ignore non-exist iscsi target
        logger.error('iscsi-target %s not found', iqn)
        raise exc.IscsiTargetNotFound(iqn=iqn)
    else:
        Target(FabricModule('iscsi'), wwn=iqn).delete()
    logger.info('delete target %s success', iqn)
    save_config()


def current_portals(iqn):
    """
    Returns list of current portals.
    """
    existing_portals = []
    for portal in _get_single_tpg(iqn).network_portals:
        existing_portals.append(
            {
                'ip': portal.ip_address,
                'port': portal.port,
            }
        )
    return existing_portals


def create_portal(iqn, ip="0.0.0.0", port=3260):
    """
    Creates portal on TPG with given iqn to destination with given
    ip and port (default 3260).
    """
    logger.info("trying to create a portal for %s", iqn)
    try:
        _get_single_tpg(iqn).network_portal(ip, port, mode='any')
    except RTSLibError as e:
        logger.error("create portal error: %s", e)
        raise exc.IscsiTargetError(action="create portal")
    save_config()


def enable_tpg(iqn, status=True):
    """
    Enables or disables the TPG. Raises an error if trying to disable a TPG
    without en enable attribute (but enabling works in that case).

    status:
        True - Enable this tpg
        False - Disable this tpg
    """
    try:
        logger.info("trying to enable tpg for target %s", iqn)
        _get_single_tpg(iqn)._set_enable(status)
    except RTSLibError as e:
        logger.error("enable tpg error: %s", e)
        raise exc.IscsiTargetError(action="enable tpg")
    logger.info("enable tpg for target %s success", iqn)
    save_config()


def current_acls(iqn):
    """
    Returns list of iqn's of current ACLs in given target
    """
    existing_acls = []
    for node_acl in _get_single_tpg(iqn).node_acls:
        existing_acls.append(
            node_acl.node_wwn
        )

    return existing_acls


def create_acl(iqn_target, iqn_initiator):
    """
    Creates new acl with given iqn, unless it already exists.
    """
    logger.info("create acl %s for target %s", iqn_initiator, iqn_target)
    tpg = _get_single_tpg(iqn_target)

    if iqn_initiator in current_acls(iqn_target):
        logger.error('acl with iqn %s already exists' % iqn_target)
        raise exc.IscsiAclExists(iqn=iqn_initiator)
    else:
        try:
            tpg.node_acl(iqn_initiator, mode='create')
        except RTSLibError as e:
            logger.error("create acl error: %s", e)
            raise exc.IscsiTargetError(action="create acl")
    logger.info("create acl %s success", iqn_initiator)
    save_config()


def get_acl(iqn, iqn_initiator):
    tpg = _get_single_tpg(iqn)
    for acl in tpg.node_acls:
        if acl.node_wwn == iqn_initiator:
            return acl
    return None


def delete_acl(iqn_target, iqn_initiator):
    acl = get_acl(iqn_target, iqn_initiator)

    # delete mapped tpg lun, acl and it's mapped lun
    logger.info("delete acl %s for target %s", iqn_initiator, iqn_target)
    if not acl:
        logger.error('acl %s not found', iqn_initiator)
        raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)
    else:
        mapped_luns = acl.mapped_luns
        for ml in mapped_luns:
            logger.debug('delete %s for acl %s', ml, iqn_initiator)
            ml.delete()
        acl.delete()
    logger.info("delete acl %s success", iqn_initiator)
    save_config()


def create_user_backstore(pool_name, disk_name, disk_size, osd_op_timeout=30,
                          disk_wwn=None, control_string=""):
    """
    Given an user backend block, only support ceph rbd now,
    checks if it already exists in the backstore or creates it.
    """
    logger.info("create user backstore %s/%s:%s, wwn: %s",
                pool_name, disk_name, disk_size, disk_wwn)
    bs = list(filter(lambda x: x['name'] == disk_name,
                     current_user_backstores()))
    if bs:
        logger.debug('backstore %s already exists', disk_name)
        raise exc.IscsiBackstoreExists(disk_name=disk_name)
    logger.info("trying to create user backstore %s/%s",
                pool_name, disk_name)
    cfgstring = "rbd/{}/{};osd_op_timeout={}".format(
        pool_name, disk_name, osd_op_timeout)
    size = convert_2_bytes(disk_size)
    try:
        so = UserBackedStorageObject(
            name=disk_name, config=cfgstring, size=size, wwn=disk_wwn,
            hw_max_sectors=1024, control=control_string)
        save_config()
        return so
    except RTSLibError as e:
        logger.error("create user backstore error: %s", e)
        raise exc.IscsiTargetError(action="create backstore")


def delete_user_backstore(disk_name):
    so = get_user_backstore(disk_name)
    if not so:
        logger.debug('user backstore %s not found, maybe delete it already',
                     disk_name)
    else:
        so.delete()
        save_config()


def get_user_backstore(disk_name):
    for so in RTSRoot().storage_objects:
        if so.name == disk_name:
            return so
    return None


def current_user_backstores():
    """
    Returns list of currently defined user backstores,
    each being represented as {'device': '<udev-path>', 'name': '<str>'}.
    """
    existing_user_backstores = []
    for backstore_object in RTSRoot().storage_objects:
        existing_user_backstores.append(
            {
                'name': backstore_object.name,
                'wwn': backstore_object.wwn,
                'size': backstore_object.size,
            }
        )
    return existing_user_backstores


def create_lun(iqn, so):
    tpg = _get_single_tpg(iqn)
    return LUN(tpg, storage_object=so)


def current_mapped_luns(iqn_target, iqn_initiator):
    """
    Returns list of currently mapped luns in given acl (resp tpg),
    each being represented as {'lun': '<id>'}.
    """
    current_tpg = _get_single_tpg(iqn_target)
    acl = get_acl(iqn_target, iqn_initiator)
    if not acl:
        logger.debug('acl %s not found', iqn_initiator)
        raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)
    mapped_luns = []
    for mapped_lun in acl.mapped_luns:
        tpg_lun = mapped_lun.tpg_lun.lun
        mapped_luns.append(
            {
                'mapped_lun': mapped_lun.mapped_lun,
                'tpg_lun': "lun{}".format(tpg_lun),
                'disk_name': LUN(current_tpg, tpg_lun).storage_object.name
            }
        )
    return mapped_luns


def remove_acl_mapped_lun(iqn_target, iqn_initiator, disk_name):
    acl = get_acl(iqn_target, iqn_initiator)
    if not acl:
        logger.error('acl %s not found', iqn_initiator)
        raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)
    acl_mapped_luns = current_mapped_luns(iqn_target, iqn_initiator)
    disk_mapped_lun_id = None
    for acl_ml in acl_mapped_luns:
        if acl_ml.get("disk_name") == disk_name:
            disk_mapped_lun_id = acl_ml.get("mapped_lun")
    logger.info("find acl %s mapped_lun%s attached to %s",
                iqn_initiator, disk_mapped_lun_id, disk_name)
    if disk_mapped_lun_id is not None:
        logger.info("delete mapped_lun %s for acl %s",
                    disk_mapped_lun_id, iqn_initiator)
        MappedLUN(acl, disk_mapped_lun_id).delete()
        for ml in acl.mapped_luns:
            if ml.mapped_lun == disk_mapped_lun_id:
                ml.delete()
        save_config()


def current_attached_luns(iqn):
    """
    Returns list of currently attached luns in given target (resp tpg),
    each being represented as:
    {
        "disk_name": "<disk_name>",
        "index": 0,
        "alias": "398884920a",
        "alua_tg_pt_gp_name": "default_tg_pt_gp"
    }
    """
    attached_luns = []
    for lun in _get_single_tpg(iqn).luns:
        attached_luns.append(
            {
                'disk_name': lun.storage_object.name,
                'lun_id': lun.lun,
                'alias': lun.alias,
                'alua_tg_pt_gp_name': lun.alua_tg_pt_gp_name
            }
        )
    return attached_luns


def create_mapped_lun(iqn, iqn_initiator, lun):
    """
    Map a lun to given acl.
    """
    logger.info("trying to map LUN %s for %s", lun, iqn)
    tpg = _get_single_tpg(iqn)
    try:
        MappedLUN(tpg.node_acl(iqn_initiator, mode='lookup'),
                  _next_free_mapped_lun_index(iqn, iqn_initiator), lun)
    except RTSLibError as e:
        logger.error("create mapped lun %s error: %s", lun, e)
        raise exc.IscsiTargetError(action="create mapped lun")
    save_config()


def set_tpg_attributes(iqn, key, value):
    if _get_single_tpg(iqn).get_attribute(key) != value:
        _get_single_tpg(iqn).set_attribute(key, value)
        logger.debug('tpg key %s has been set to value %s', key, value)
    save_config()

    return None


def chap_disable(iqn):
    set_tpg_attributes(iqn, "authentication", 0)
    save_config()


def mutual_chap_enable(iqn_target, mutual_username, mutual_password):
    """
    Set mutual chap for all acls in a iscsi target
    """
    set_tpg_attributes(iqn_target, "authentication", 1)
    tpg = _get_single_tpg(iqn_target)
    for acl in tpg.node_acls:
        acl.chap_mutual_userid = mutual_username
        acl.chap_mutual_password = mutual_password
    save_config()


def set_acl_mutual_chap(iqn_target, iqn_initiator, mutual_username,
                        mutual_password):
    """
    Set mutual chap for a specified acl in a iscsi target
    If mutual_username and mutual_password is "", will disable
    """
    logger.debug("%s: trying to set mutual chap for acl: %s",
                 iqn_target, iqn_initiator)
    set_tpg_attributes(iqn_target, "authentication", 1)
    acl = get_acl(iqn_target, iqn_initiator)
    if not acl:
        logger.error("can't find the specified acl: %s", iqn_initiator)
        raise exc.IscsiAclNotFound(iqn_initiator=iqn_initiator)
    acl.chap_mutual_userid = mutual_username
    acl.chap_mutual_password = mutual_password


def chap_enable(iqn, username, password):
    set_tpg_attributes(iqn, "authentication", 1)
    tpg = _get_single_tpg(iqn)
    for acl in tpg.node_acls:
        acl.chap_userid = username
        acl.chap_password = password
    save_config()


def get_lun_id(iqn, disk_name):
    for curlun in current_attached_luns(iqn):
        if disk_name == curlun['disk_name']:
            return curlun.get('lun_id')


def _next_free_mapped_lun_index(iqn, iqn_initiator):
    """
    Returns a non-allocated mapped_lun index.
    """
    mapped_luns = current_mapped_luns(iqn, iqn_initiator)
    mapped_lun_indices = [
        mapped_lun['mapped_lun'] for mapped_lun in mapped_luns
    ]
    if not mapped_lun_indices:
        next_free_index = 0
    else:
        next_free_index = max(mapped_lun_indices) + 1

    return next_free_index


def _get_single_tpg(iqn):
    """
    Returns TPG object for given iqn, assuming that each Target
    has a single TPG.
    """
    tpg = TPG(Target(FabricModule('iscsi'), iqn, mode="lookup"), 1)
    return tpg


def save_config(savefile=DEFAULT_SAVE_FILE):
    '''
    Saves the current configuration to a file so that it can be restored
    on next boot.
    '''
    if not savefile:
        savefile = DEFAULT_SAVE_FILE
    savefile = os.path.expanduser(savefile)
    _save_backups(savefile)
    RTSRoot().save_to_file(savefile)
    logger.info("Configuration saved to %s", savefile)


def _save_backups(savefile):
    '''
    Take backup of config-file if needed.
    '''
    # Only save backups if saving to default location
    if savefile != DEFAULT_SAVE_FILE:
        return

    backup_dir = os.path.dirname(savefile) + "/backup/"
    backup_name = "saveconfig-" + datetime.datetime.now().strftime(
        "%Y%m%d-%H:%M:%S") + ".json"
    backupfile = backup_dir + backup_name
    backup_error = None

    if not os.path.exists(backup_dir):
        try:
            os.makedirs(backup_dir)
        except OSError as exe:
            logger.error("Cannot create backup directory %s: %s",
                         backup_dir, exe.strerror)
            raise exc.IscsiTargetError(action="saveconfig")

    # Only save backups if savefile exits
    if not os.path.exists(savefile):
        return

    backed_files_list = sorted(glob(
        os.path.dirname(savefile) + "/backup/*.json"))

    # Save backup if backup dir is empty, or savefile is differnt from
    # recent backup copy
    if not backed_files_list or not filecmp.cmp(backed_files_list[-1],
                                                savefile):
        try:
            shutil.copy(savefile, backupfile)

        except IOError as ioe:
            backup_error = ioe.strerror or "Unknown error"

        if not backup_error:
            # remove excess backups
            max_backup_files = 256
            files_to_unlink = list(reversed(
                backed_files_list))[max_backup_files - 1:]
            for f in files_to_unlink:
                with ignored(IOError):
                    os.unlink(f)

            logger.info("Last %s configs saved in %s.",
                        max_backup_files, backup_dir)
        else:
            logger.error("Could not create backup file %s: %s",
                         backupfile, backup_error)
            raise exc.IscsiTargetError(action="create backup file")

#############################################################################


def test_create():
    iqn = "iqn.2003-01.org.linux-iscsi.ceph-3.x8664:sn.5dff8f6e764b"
    clients = ["iqn.2019-09.dspacee.net:0008",
               "iqn.2019-09.dspacee.net:0009"]
    disk_name = ["rbd-002"]
    if iqn not in current_targets():
        create_target(iqn)
        create_portal(iqn, "192.168.17.54")
        enable_tpg(iqn, True)
    for iqn_initiator in clients:
        create_acl(iqn, iqn_initiator)
        for vol in disk_name:
            so = None
            so = get_user_backstore(vol)
            if not so:
                so = create_user_backstore("rbd", vol, "2G")
            lun = create_lun(iqn, so)
            create_mapped_lun(iqn, iqn_initiator, lun)
    chap_enable(iqn, "username", "password")
    cur_attached_luns = current_attached_luns(iqn)
    mapped_luns = current_mapped_luns(iqn, iqn_initiator)
    print("mapped_luns: {}".format(json.dumps(mapped_luns)))
    print("cur_attached_luns: {}".format(json.dumps(cur_attached_luns)))


def test_delete():
    iqn = "iqn.2003-01.org.linux-iscsi.ceph-3.x8664:sn.5dff8f6e764b"
    iqn_initiator1 = "iqn.2019-09.dspacee.net:0008"
    iqn_initiator2 = "iqn.2019-09.dspacee.net:0009"
    disk_name = "test.img"
    delete_acl(iqn, iqn_initiator1)
    delete_acl(iqn, iqn_initiator2)
    delete_user_backstore(disk_name)
    delete_target(iqn)


if __name__ == '__main__':
    test_create()
    test_delete()
