#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
from oslo_utils import timeutils
import pytz

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects


fake_rpc_service = {
    'id': 1,
    'service_name': "admin",
    'hostname': "devel",
    'cluster_id': 'defalut',
    'endpoint': '{}',
}


class TestRPCService(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_rpc_service)
    def test_get_by_id(self, rpc_service_get):
        rpc_service = objects.RPCService.get_by_id(self.context,
                                                   fake_rpc_service['id'])
        self._compare(self, fake_rpc_service, rpc_service)
        rpc_service_get.assert_called_once_with(
            self.context, "RPCService", fake_rpc_service['id'])

    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query):
        model_query().filter_by().first.return_value = None
        self.assertRaises(exception.RPCServiceNotFound,
                          objects.RPCService.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.rpc_service_create', return_value=fake_rpc_service)
    def test_create(self, rpc_service_create):
        rpc_service = objects.RPCService(context=self.context)
        rpc_service.create()
        self.assertEqual(fake_rpc_service['id'], rpc_service.id)
        self.assertEqual(fake_rpc_service['hostname'],
                         fake_rpc_service['hostname'])

    @mock.patch('t2stor.db.rpc_service_update')
    def test_save(self, rpc_service_update):
        rpc_service = objects.RPCService._from_db_object(
            self.context, objects.RPCService(), fake_rpc_service)
        rpc_service.hostname = 'foobar'
        rpc_service.save()
        rpc_service_update.assert_called_once_with(
            self.context, rpc_service.id, {'hostname': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.rpc_service_destroy')
    def test_destroy(self, rpc_service_destroy, utcnow_mock):
        rpc_service_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        rpc_service = objects.RPCService(context=self.context,
                                         id=fake_rpc_service['id'])
        rpc_service.destroy()
        self.assertTrue(rpc_service_destroy.called)
        self.assertTrue(rpc_service.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         rpc_service.deleted_at)


class TestRPCServiceList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.rpc_service_get_all',
                return_value=[fake_rpc_service])
    def test_get_all(self, rpc_service_get_all):
        rpc_services = objects.RPCServiceList.get_all(self.context)
        self.assertEqual(1, len(rpc_services))
        TestRPCService._compare(self, fake_rpc_service, rpc_services[0])