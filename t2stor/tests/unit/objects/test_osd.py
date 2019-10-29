#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
import pytz
from oslo_utils import timeutils

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_osd = {
    'id': 1,
    'name': "devel",
    'type': "bluestore",
    'disk_type': "hdd",
    'status': "up",
    'cluster_id': "3fc66dde-6c6b-42d2-983b-930198d0c2f5",
    'pool_id': 1
}


class TestOsd(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_osd)
    def test_get_by_id(self, osd_get):
        osd = objects.Osd.get_by_id(self.context,
                                    fake_osd['id'])
        self._compare(self, fake_osd, osd)
        osd_get.assert_called_once_with(
            self.context, "Osd", fake_osd['id'], None)

    @mock.patch('t2stor.db.sqlalchemy.api.get_session')
    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query, get_session):
        get_session().return_value = mock.MagicMock()
        model_query().filter_by().first.return_value = None
        self.assertRaises(exception.OsdNotFound,
                          objects.Osd.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.osd_create', return_value=fake_osd)
    def test_create(self, osd_create):
        osd = objects.Osd(context=self.context)
        osd.create()
        self.assertEqual(fake_osd['id'], osd.id)
        self.assertEqual(fake_osd['name'],
                         osd.name)

    @mock.patch('t2stor.db.osd_update')
    def test_save(self, osd_update):
        osd = objects.Osd._from_db_object(
            self.context, objects.Osd(), fake_osd)
        osd.name = 'foobar'
        osd.save()
        osd_update.assert_called_once_with(
            self.context, osd.id, {'name': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.osd_destroy')
    def test_destroy(self, osd_destroy, utcnow_mock):
        osd_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        osd = objects.Osd(context=self.context,
                          id=fake_osd['id'])
        osd.destroy()
        self.assertTrue(osd_destroy.called)
        self.assertTrue(osd.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         osd.deleted_at)


class TestOsdList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.osd_get_all',
                return_value=[fake_osd])
    def test_get_all(self, osd_get_all):
        osds = objects.OsdList.get_all(self.context)
        self.assertEqual(1, len(osds))
        self.assertIsInstance(osds[0], objects.Osd)
        self._compare(self, fake_osd, osds[0])
