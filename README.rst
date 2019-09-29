=================
Stor Project
=================

Services:

1. api: Provide API interface for UI.

  python3 -m stor.api.api --config-file build/api.ini

2. admin: Deploy and admin ceph clusters.

  python3 -m stor.admin.admin --config-file build/admin.ini

3. agent: Manage node.

  python3 -m stor.agent.agent --config-file build/agent.ini


Tools
=====

DB Sync:

  python3 -m stor.cmd.manage --config-file build/api.ini db sync

Tests
=====
  python3 -m unittest stor.tests.unit
