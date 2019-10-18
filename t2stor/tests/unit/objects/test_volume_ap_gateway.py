#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
from oslo_utils import timeutils
import pytz

# from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects


fake_volume_ap_gateway = {
    'id': 1,
    'iqn': "iqn.2019-10.t2store.net:2990bfd9e03e",
    'node_id': 1,
    "volume_access_path_id": 1
}


class TestVolumeAPGateway(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.volume_ap_gateway_create',
                return_value=fake_volume_ap_gateway)
    def test_create(self, volume_ap_gateway_create):
        ap_gateway = objects.VolumeAPGateway(context=self.context)
        ap_gateway.create()
        self.assertEqual(fake_volume_ap_gateway['id'], ap_gateway.id)
        self.assertEqual(fake_volume_ap_gateway['iqn'], ap_gateway.iqn)
        self.assertEqual(fake_volume_ap_gateway['node_id'], ap_gateway.node_id)
        self.assertEqual(fake_volume_ap_gateway['volume_access_path_id'],
                         ap_gateway.volume_access_path_id)

    @mock.patch('t2stor.db.volume_ap_gateway_update')
    def test_save(self, volume_ap_gateway_update):
        ap_gateway = objects.VolumeAPGateway._from_db_object(
            self.context, objects.VolumeAPGateway(), fake_volume_ap_gateway)
        ap_gateway.iqn = 'iqn.2019-10.t2store.net:2990bfd9e03e'
        ap_gateway.save()
        volume_ap_gateway_update.assert_called_once_with(
            self.context, ap_gateway.id,
            {'iqn': 'iqn.2019-10.t2store.net:2990bfd9e03e'})

    @mock.patch('oslo_utils.timeutils.utcnow',
                return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.volume_ap_gateway_destroy')
    def test_destroy(self, volume_ap_gateway_destroy, utcnow_mock):
        volume_ap_gateway_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        ap_gateway = objects.VolumeAPGateway(
            context=self.context, id=fake_volume_ap_gateway['id'])
        ap_gateway.destroy()
        self.assertTrue(volume_ap_gateway_destroy.called)
        self.assertTrue(ap_gateway.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         ap_gateway.deleted_at)


class TestAPGatewayList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.volume_ap_gateway_get_all',
                return_value=[fake_volume_ap_gateway])
    def test_get_all(self, volume_ap_gateway_get_all):
        ap_gateways = objects.VolumeAPGatewayList.get_all(self.context)
        self.assertEqual(1, len(ap_gateways))
        self.assertIsInstance(ap_gateways[0], objects.VolumeAPGateway)
        self._compare(self, fake_volume_ap_gateway, ap_gateways[0])
