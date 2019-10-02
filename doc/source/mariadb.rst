============
Mariadb
============

Create DB and User
==================

Create db and user

.. code-block:: sql
   :caption: prepare.sql
   :name: prepare.sql
  
   CREATE DATABASE stor;
   GRANT ALL PRIVILEGES ON stor.* TO 'stor'@'localhost' \
     IDENTIFIED BY 'STOR_DBPASS';
   GRANT ALL PRIVILEGES ON stor.* TO 'stor'@'%' \
     IDENTIFIED BY 'STOR_DBPASS';

Init db

.. code-block:: shell
   :caption: init.sh
   :name: init.sh

   python3 -m stor.cmd.manage --config-file build/stor.ini db sync
