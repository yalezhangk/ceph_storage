#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import pytz
from oslo_utils import timeutils

# from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_volume_client = {
    'id': 1,
    'client_type': "iscsi",
    'iqn': "iqn.2019-10.t2store.net:2990bfd9e03e",
    'volume_client_group_id': 1
}


class TestVolumeAccessPath(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.volume_client_create',
                return_value=fake_volume_client)
    def test_create(self, volume_client_create):
        volume_client = objects.VolumeClient(context=self.context)
        volume_client.create()
        self.assertEqual(fake_volume_client['id'], volume_client.id)
        self.assertEqual(fake_volume_client['iqn'], volume_client.iqn)

    @mock.patch('t2stor.db.volume_client_update')
    def test_save(self, volume_client_update):
        volume_client = objects.VolumeClient._from_db_object(
            self.context, objects.VolumeClient(), fake_volume_client)
        volume_client.iqn = 'iqn.2019-10.t2store.net:2990bfd9e03e'
        volume_client.save()
        volume_client_update.assert_called_once_with(
            self.context, volume_client.id,
            {'iqn': 'iqn.2019-10.t2store.net:2990bfd9e03e'})

    @mock.patch('oslo_utils.timeutils.utcnow',
                return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.volume_client_destroy')
    def test_destroy(self, volume_client_destroy, utcnow_mock):
        volume_client_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        volume_client = objects.VolumeClient(
            context=self.context, id=fake_volume_client['id'])
        volume_client.destroy()
        self.assertTrue(volume_client_destroy.called)
        self.assertTrue(volume_client_destroy.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         volume_client.deleted_at)


class TestVolumeClientList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.volume_client_get_all',
                return_value=[fake_volume_client])
    def test_get_all(self, volume_client_get_all):
        volume_clients = objects.VolumeClientList.get_all(self.context)
        self.assertEqual(1, len(volume_clients))
        self.assertIsInstance(volume_clients[0], objects.VolumeClient)
        self._compare(self, fake_volume_client, volume_clients[0])
