#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
import pytz
from oslo_utils import timeutils

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_ceph_config = {
    'id': 1,
    'group': "DEFAULT",
    'key': "devel",
    'value': "value",
    'cluster_id': "8466c699-42c4-4abb-bae2-19b3ef0d8b90"
}


class TestCephConfig(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_ceph_config)
    def test_get_by_id(self, ceph_config_get):
        ceph_config = objects.CephConfig.get_by_id(self.context,
                                                   fake_ceph_config['id'])
        self._compare(self, fake_ceph_config, ceph_config)
        ceph_config_get.assert_called_once_with(
            self.context, "CephConfig", fake_ceph_config['id'])

    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query):
        model_query().filter_by().filter_by().first.return_value = None
        self.assertRaises(exception.CephConfigNotFound,
                          objects.CephConfig.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.ceph_config_create', return_value=fake_ceph_config)
    def test_create(self, ceph_config_create):
        ceph_config = objects.CephConfig(context=self.context)
        ceph_config.create()
        self.assertEqual(fake_ceph_config['id'], ceph_config.id)
        self.assertEqual(fake_ceph_config['key'],
                         ceph_config.key)

    @mock.patch('t2stor.db.ceph_config_update')
    def test_save(self, ceph_config_update):
        ceph_config = objects.CephConfig._from_db_object(
            self.context, objects.CephConfig(), fake_ceph_config)
        ceph_config.key = 'foobar'
        ceph_config.save()
        ceph_config_update.assert_called_once_with(
            self.context, ceph_config.id, {'key': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.ceph_config_destroy')
    def test_destroy(self, ceph_config_destroy, utcnow_mock):
        ceph_config_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        ceph_config = objects.CephConfig(context=self.context,
                                         id=fake_ceph_config['id'])
        ceph_config.destroy()
        self.assertTrue(ceph_config_destroy.called)
        self.assertTrue(ceph_config.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         ceph_config.deleted_at)


class TestCephConfigList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.ceph_config_get_all',
                return_value=[fake_ceph_config])
    def test_get_all(self, ceph_config_get_all):
        ceph_configs = objects.CephConfigList.get_all(self.context)
        self.assertEqual(1, len(ceph_configs))
        self.assertIsInstance(ceph_configs[0], objects.CephConfig)
        self._compare(self, fake_ceph_config, ceph_configs[0])
