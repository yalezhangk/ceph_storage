#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import pytz
from oslo_utils import timeutils

# from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_volume_client_group = {
    'id': 1,
    'name': "client-0001",
    "type": "iscsi",
    "chap_enable": True,
    "chap_username": "username",
    "chap_password": "password",
    "access_path_id": 1
}


class TestVolumeClientGroup(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.volume_client_group_create',
                return_value=fake_volume_client_group)
    def test_create(self, volume_client_group_create):
        client_group = objects.VolumeClientGroup(context=self.context)
        client_group.create()
        self.assertEqual(fake_volume_client_group['id'], client_group.id)
        self.assertEqual(fake_volume_client_group['name'], client_group.name)

    @mock.patch('t2stor.db.volume_client_group_update')
    def test_save(self, volume_client_group_update):
        client_group = objects.VolumeClientGroup._from_db_object(
            self.context, objects.VolumeClientGroup(),
            fake_volume_client_group)
        client_group.name = 'client-0001'
        client_group.save()
        volume_client_group_update.assert_called_once_with(
            self.context, client_group.id, {'name': 'client-0001'})

    @mock.patch('oslo_utils.timeutils.utcnow',
                return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.volume_client_group_destroy')
    def test_destroy(self, volume_client_group_destroy, utcnow_mock):
        volume_client_group_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        client_group = objects.VolumeClientGroup(
            context=self.context, id=fake_volume_client_group['id'])
        client_group.destroy()
        self.assertTrue(volume_client_group_destroy.called)
        self.assertTrue(client_group.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         client_group.deleted_at)


class TestVolumeClientGroupList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.volume_client_group_get_all',
                return_value=[fake_volume_client_group])
    def test_get_all(self, volume_client_group_get_all):
        client_groups = objects.VolumeClientGroupList.get_all(self.context)
        self.assertEqual(1, len(client_groups))
        self.assertIsInstance(client_groups[0], objects.VolumeClientGroup)
        self._compare(self, fake_volume_client_group, client_groups[0])
