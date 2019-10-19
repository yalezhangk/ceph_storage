#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
from oslo_utils import timeutils
import pytz

# from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects


fake_volume_gateway = {
    'id': 1,
    'iqn': "iqn.2019-10.t2store.net:2990bfd9e03e",
    'node_id': 1,
    "volume_access_path_id": 1
}


class TestVolumeGateway(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.volume_gateway_create',
                return_value=fake_volume_gateway)
    def test_create(self, volume_gateway_create):
        gateway = objects.VolumeGateway(context=self.context)
        gateway.create()
        self.assertEqual(fake_volume_gateway['id'], gateway.id)
        self.assertEqual(fake_volume_gateway['iqn'], gateway.iqn)
        self.assertEqual(fake_volume_gateway['node_id'], gateway.node_id)
        self.assertEqual(fake_volume_gateway['volume_access_path_id'],
                         gateway.volume_access_path_id)

    @mock.patch('t2stor.db.volume_gateway_update')
    def test_save(self, volume_gateway_update):
        gateway = objects.VolumeGateway._from_db_object(
            self.context, objects.VolumeGateway(), fake_volume_gateway)
        gateway.iqn = 'iqn.2019-10.t2store.net:2990bfd9e03e'
        gateway.save()
        volume_gateway_update.assert_called_once_with(
            self.context, gateway.id,
            {'iqn': 'iqn.2019-10.t2store.net:2990bfd9e03e'})

    @mock.patch('oslo_utils.timeutils.utcnow',
                return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.volume_gateway_destroy')
    def test_destroy(self, volume_gateway_destroy, utcnow_mock):
        volume_gateway_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        gateway = objects.VolumeGateway(
            context=self.context, id=fake_volume_gateway['id'])
        gateway.destroy()
        self.assertTrue(volume_gateway_destroy.called)
        self.assertTrue(gateway.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         gateway.deleted_at)


class TestGatewayList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.volume_gateway_get_all',
                return_value=[fake_volume_gateway])
    def test_get_all(self, volume_gateway_get_all):
        gateways = objects.VolumeGatewayList.get_all(self.context)
        self.assertEqual(1, len(gateways))
        self.assertIsInstance(gateways[0], objects.VolumeGateway)
        self._compare(self, fake_volume_gateway, gateways[0])
