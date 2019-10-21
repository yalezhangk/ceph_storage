#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import pytz
from oslo_utils import timeutils

# from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_volume_access_path = {
    'id': 1,
    'iqn': "iqn.2019-10.t2store.net:2990bfd9e03e",
    'name': "target-0001",
    "status": "active",
    "type": "iscsi",
    "chap_enable": True,
    "chap_username": "username",
    "chap_password": "password"
}


class TestVolumeAccessPath(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.volume_access_path_create',
                return_value=fake_volume_access_path)
    def test_create(self, volume_access_path_create):
        access_path = objects.VolumeAccessPath(context=self.context)
        access_path.create()
        self.assertEqual(fake_volume_access_path['id'], access_path.id)
        self.assertEqual(fake_volume_access_path['name'], access_path.name)

    @mock.patch('t2stor.db.volume_access_path_update')
    def test_save(self, volume_access_path_update):
        access_path = objects.VolumeAccessPath._from_db_object(
            self.context, objects.VolumeAccessPath(), fake_volume_access_path)
        access_path.name = 'target-0002'
        access_path.save()
        volume_access_path_update.assert_called_once_with(
            self.context, access_path.id, {'name': 'target-0002'})

    @mock.patch('oslo_utils.timeutils.utcnow',
                return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.volume_access_path_destroy')
    def test_destroy(self, volume_access_path_destroy, utcnow_mock):
        volume_access_path_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        volume_access_path = objects.VolumeAccessPath(
            context=self.context, id=fake_volume_access_path['id'])
        volume_access_path.destroy()
        self.assertTrue(volume_access_path_destroy.called)
        self.assertTrue(volume_access_path.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         volume_access_path.deleted_at)


class TestAccessPathList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.volume_access_path_get_all',
                return_value=[fake_volume_access_path])
    def test_get_all(self, volume_access_path_get_all):
        access_paths = objects.VolumeAccessPathList.get_all(self.context)
        self.assertEqual(1, len(access_paths))
        self.assertIsInstance(access_paths[0], objects.VolumeAccessPath)
        self._compare(self, fake_volume_access_path, access_paths[0])
