=================
Stor Project
=================

Services:

1. api: Provide API interface for UI.

  python3 -m t2stor.cmd.api --config-file build/api.conf

2. admin: Deploy and admin ceph clusters.

  python3 -m t2stor.cmd.admin --config-file build/admin.conf

3. agent: Manage node.

  python3 -m t2stor.cmd.agent --config-file build/agent.conf


Tools
=====

DB Sync:

  python3 -m t2stor.cmd.manage --config-file build/api.conf db sync

Tests
=====
Simple test file
  
  python3 -m unittest t2stor.tests.unit.objects.test_node

All tests
 
  python3 -m stestr --test-path t2stor.tests.unit run
