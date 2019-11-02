=================
Stor Project
=================

Services:

1. api: Provide API interface for UI.

  cp etc/t2stor/t2stor.conf build/api.conf
  python3 -m t2stor.cmd.api --config-file build/api.conf

2. admin: Deploy and admin ceph clusters.

  cp etc/t2stor/t2stor.conf build/admin.conf
  python3 -m t2stor.cmd.admin --config-file build/admin.conf

3. agent: Manage node.

  cp etc/t2stor/t2stor.conf build/agent.conf
  python3 -m t2stor.cmd.agent --config-file build/agent.conf


Tools
=====

DB Sync:

  python3 -m t2stor.cmd.manage --config-file build/api.conf db sync

Code Style
===========

import style

.. code-block:: shell

  isort -rc -ns __init__.py --force-single-line-imports server/

- line width 79


Commit Style
=============

.. code-block:: shell

  type(module): what's changed
  
  full message ...


type:

- fe: feature
- re: refactor
- fix: fix bug

Example:

.. code-block:: shell

  fe(osd): add deploy osd
  
  Support deploy osd, include cache, wal, db, journal.


Tests
=====
Simple unit test file

  python3 -m unittest t2stor.tests.unit.objects.test_node

All unit tests

  python3 -m stestr --test-path t2stor.tests.unit run

Tox:

.. code-block:: shell

  tox


More
=====
See doc
