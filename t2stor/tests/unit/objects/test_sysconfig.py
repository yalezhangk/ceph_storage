#!/usr/bin/env python
# -*- coding: utf-8 -*-


import mock
import pytz
from oslo_utils import timeutils

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_sys_config = {
    'id': 1,
    'service_id': "devel",
    'key': "key",
    'value': "value",
    'value_type': "value_type",
    'display_description': "display_description",
}


class TestSysConfig(test_objects.BaseObjectsTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_sys_config)
    def test_get_by_id(self, sys_config_get):
        sys_config = objects.SysConfig.get_by_id(self.context,
                                                 fake_sys_config['id'])
        self._compare(self, fake_sys_config, sys_config)
        sys_config_get.assert_called_once_with(
            self.context, "SysConfig", fake_sys_config['id'])

    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query):
        model_query().filter_by().first.return_value = None
        self.assertRaises(exception.SysConfigNotFound,
                          objects.SysConfig.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.sys_config_create', return_value=fake_sys_config)
    def test_create(self, sys_config_create):
        sys_config = objects.SysConfig(context=self.context)
        sys_config.create()
        self.assertEqual(fake_sys_config['id'], sys_config.id)
        self.assertEqual(fake_sys_config['key'],
                         sys_config.key)

    @mock.patch('t2stor.db.sys_config_update')
    def test_save(self, sys_config_update):
        sys_config = objects.SysConfig._from_db_object(
            self.context, objects.SysConfig(), fake_sys_config)
        sys_config.key = 'foobar'
        sys_config.save()
        sys_config_update.assert_called_once_with(
            self.context, sys_config.id, {'key': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.sys_config_destroy')
    def test_destroy(self, sys_config_destroy, utcnow_mock):
        sys_config_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        sys_config = objects.SysConfig(context=self.context,
                                       id=fake_sys_config['id'])
        sys_config.destroy()
        self.assertTrue(sys_config_destroy.called)
        self.assertTrue(sys_config.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         sys_config.deleted_at)


class TestSysConfigList(test_objects.BaseObjectsTestCase):
    @mock.patch('t2stor.db.sys_config_get_all',
                return_value=[fake_sys_config])
    def test_get_all(self, sys_config_get_all):
        sys_configs = objects.SysConfigList.get_all(self.context)
        self.assertEqual(1, len(sys_configs))
        self.assertIsInstance(sys_configs[0], objects.SysConfig)
        self._compare(self, fake_sys_config, sys_configs[0])
