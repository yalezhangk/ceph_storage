#!/usr/bin/env python
# -*- coding: utf-8 -*-


import copy

import mock
import pytz
from netaddr import IPAddress
from oslo_utils import timeutils

from t2stor import exception
from t2stor import objects
from t2stor.tests.unit import objects as test_objects

fake_node = {
    'id': 1,
    'cluster_id': "3fc66dde-6c6b-42d2-983b-930198d0c2f5",
    'ip_address': "192.168.0.1",
    'gateway_ip_address': "192.168.0.1",
    'storage_cluster_ip_address': "192.168.0.1",
    'storage_public_ip_address': "192.168.0.1",
    'status': "creating",
    'role_base': False,
    'role_admin': False,
    'role_monitor': False,
    'role_storage': False,
    'role_block_gateway': False,
    'role_object_gateway': False,
    'role_file_gateway': False,
    'hostname': "devel",
}


class NodeCompareTestCase(test_objects.BaseObjectsTestCase):
    def _compare(self, fake_node, node):
        ins_node = copy.deepcopy(fake_node)
        for attr in ['ip_address', 'gateway_ip_address',
                     'storage_cluster_ip_address',
                     'storage_public_ip_address']:
            ins_node[attr] = IPAddress(fake_node[attr])
        super(NodeCompareTestCase, self)._compare(self, ins_node, node)


class TestNode(NodeCompareTestCase):

    @mock.patch('t2stor.db.get_by_id', return_value=fake_node)
    def test_get_by_id(self, node_get):
        node = objects.Node.get_by_id(self.context,
                                      fake_node['id'])
        self._compare(fake_node, node)
        node_get.assert_called_once_with(
            self.context, "Node", fake_node['id'], None)

    @mock.patch('t2stor.db.sqlalchemy.api.get_session')
    @mock.patch('t2stor.db.sqlalchemy.api.model_query')
    def test_get_by_id_no_existing_id(self, model_query, get_session):
        get_session().return_value = mock.MagicMock()
        model_query().filter_by().filter_by().first.return_value = None
        self.assertRaises(exception.NodeNotFound,
                          objects.Node.get_by_id,
                          self.context, 123)

    @mock.patch('t2stor.db.node_create', return_value=fake_node)
    def test_create(self, node_create):
        node = objects.Node(context=self.context)
        node.create()
        self.assertEqual(fake_node['id'], node.id)
        self.assertEqual(fake_node['hostname'],
                         node.hostname)

    @mock.patch('t2stor.db.node_update')
    def test_save(self, node_update):
        node = objects.Node._from_db_object(
            self.context, objects.Node(), fake_node)
        node.hostname = 'foobar'
        node.save()
        node_update.assert_called_once_with(
            self.context, node.id, {'hostname': 'foobar'})

    @mock.patch('oslo_utils.timeutils.utcnow', return_value=timeutils.utcnow())
    @mock.patch('t2stor.db.sqlalchemy.api.node_destroy')
    def test_destroy(self, node_destroy, utcnow_mock):
        node_destroy.return_value = {
            'deleted': True,
            'deleted_at': utcnow_mock.return_value}
        node = objects.Node(context=self.context,
                            id=fake_node['id'])
        node.destroy()
        self.assertTrue(node_destroy.called)
        self.assertTrue(node.deleted)
        self.assertEqual(utcnow_mock.return_value.replace(tzinfo=pytz.UTC),
                         node.deleted_at)


class TestNodeList(NodeCompareTestCase):
    @mock.patch('t2stor.db.node_get_all',
                return_value=[fake_node])
    def test_get_all(self, node_get_all):
        nodes = objects.NodeList.get_all(self.context)
        self.assertEqual(1, len(nodes))
        self.assertIsInstance(nodes[0], objects.Node)
        self._compare(fake_node, nodes[0])
