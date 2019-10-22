#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
import pytz
from oslo_utils import timeutils

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_rack = {
    'id': 1,
    'name': "devel",
    'datacenter_id': 1,
    'cluster_id': "8466c699-42c4-4abb-bae2-19b3ef0d8b90"
}


class TestRack(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_rack)
    def test_get_by_id(self, rack_get):
        rack = objects.Rack.get_by_id(self.context,
                                      fake_rack['id'])
        self._compare(self, fake_rack, rack)
        rack_get.assert_called_once_with(
            self.context, "Rack", fake_rack['id'])

    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query):
        model_query().filter_by().first.return_value = None
        self.assertRaises(exception.RackNotFound,
                          objects.Rack.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.rack_create', return_value=fake_rack)
    def test_create(self, rack_create):
        rack = objects.Rack(context=self.context)
        rack.create()
        self.assertEqual(fake_rack['id'], rack.id)
        self.assertEqual(fake_rack['name'],
                         rack.name)

    @mock.patch('t2stor.db.rack_update')
    def test_save(self, rack_update):
        rack = objects.Rack._from_db_object(
            self.context, objects.Rack(), fake_rack)
        rack.name = 'foobar'
        rack.save()
        rack_update.assert_called_once_with(
            self.context, rack.id, {'name': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.rack_destroy')
    def test_destroy(self, rack_destroy, utcnow_mock):
        rack_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        rack = objects.Rack(context=self.context,
                            id=fake_rack['id'])
        rack.destroy()
        self.assertTrue(rack_destroy.called)
        self.assertTrue(rack.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         rack.deleted_at)


class TestRackList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.rack_get_all',
                return_value=[fake_rack])
    def test_get_all(self, rack_get_all):
        racks = objects.RackList.get_all(self.context)
        self.assertEqual(1, len(racks))
        self.assertIsInstance(racks[0], objects.Rack)
        self._compare(self, fake_rack, racks[0])
