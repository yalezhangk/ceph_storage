#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
import pytz
from oslo_utils import timeutils

from DSpace import exception
from DSpace import objects
from DSpace.tests.unit import objects as test_objects

fake_datacenter = {
    'id': 1,
    'name': "devel",
    'cluster_id': "8466c699-42c4-4abb-bae2-19b3ef0d8b90",
}


class TestDatacenter(test_objects.BaseObjectsTestCase):

    @mock.patch('DSpace.db.get_by_id', return_value=fake_datacenter)
    def test_get_by_id(self, datacenter_get):
        datacenter = objects.Datacenter.get_by_id(self.context,
                                                  fake_datacenter['id'])
        self._compare(self, fake_datacenter, datacenter)
        datacenter_get.assert_called_once_with(
            self.context, "Datacenter", fake_datacenter['id'])

    @mock.patch('DSpace.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query):
        model_query().filter_by().first.return_value = None
        self.assertRaises(exception.DatacenterNotFound,
                          objects.Datacenter.get_by_id,
                          self.context, 123)

    @mock.patch('DSpace.db.datacenter_create', return_value=fake_datacenter)
    def test_create(self, datacenter_create):
        datacenter = objects.Datacenter(context=self.context)
        datacenter.create()
        self.assertEqual(fake_datacenter['id'], datacenter.id)
        self.assertEqual(fake_datacenter['name'],
                         datacenter.name)

    @mock.patch('DSpace.db.datacenter_update')
    def test_save(self, datacenter_update):
        datacenter = objects.Datacenter._from_db_object(
            self.context, objects.Datacenter(), fake_datacenter)
        datacenter.name = 'foobar'
        datacenter.save()
        datacenter_update.assert_called_once_with(
            self.context, datacenter.id, {'name': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('DSpace.db.sqlalchemy.api.datacenter_destroy')
    def test_destroy(self, datacenter_destroy, utcnow_mock):
        datacenter_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        datacenter = objects.Datacenter(context=self.context,
                                        id=fake_datacenter['id'])
        datacenter.destroy()
        self.assertTrue(datacenter_destroy.called)
        self.assertTrue(datacenter.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         datacenter.deleted_at)


class TestDatacenterList(test_objects.BaseObjectsTestCase):
    @mock.patch('DSpace.db.datacenter_get_all',
                return_value=[fake_datacenter])
    def test_get_all(self, datacenter_get_all):
        datacenters = objects.DatacenterList.get_all(self.context)
        self.assertEqual(1, len(datacenters))
        self.assertIsInstance(datacenters[0], objects.Datacenter)
        self._compare(self, fake_datacenter, datacenters[0])
