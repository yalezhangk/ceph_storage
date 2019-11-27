=================
Stor Project
=================

Services:

1. api: Provide API interface for UI.

  cp etc/dspace/dspace.conf build/api.conf
  python3 -m DSpace.cmd.api --config-file build/api.conf

2. admin: Deploy and admin ceph clusters.

  cp etc/dspace/dspace.conf build/admin.conf
  python3 -m DSpace.cmd.admin --config-file build/admin.conf

3. agent: Manage node.

  cp etc/dspace/dspace.conf build/agent.conf
  python3 -m DSpace.cmd.agent --config-file build/agent.conf


Tools
=====

DB Sync:

  python3 -m DSpace.cmd.manage --config-file build/api.conf db sync

Code Style
===========

import style

.. code-block:: shell

  isort -rc -ns __init__.py --force-single-line-imports DSpace/

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


I18n
====

.. code-block:: shell

  # create msg file
  pybabel extract     --add-comments Translators:  \
    -k "_C:1c,2" -k "_P:1,2"     --project=space --version=1.0 \
    -o DSpace/locale/dspace.pot DSpace/

  # merge msg
  msgmerge -o DSpace/locale/zh_CN/LC_MESSAGES/dspace.po \
    DSpace/locale/zh_CN/LC_MESSAGES/dspace.po \
    DSpace/locale/dspace.pot

  # delete commit
  sed -i "/^#/d" DSpace/locale/zh_CN/LC_MESSAGES/dspace.po



Tests
=====
Simple unit test file

  python3 -m unittest DSpace.tests.unit.objects.test_node

All unit tests

  python3 -m stestr --test-path DSpace.tests.unit run

Tox:

.. code-block:: shell

  tox


More
=====
See doc
