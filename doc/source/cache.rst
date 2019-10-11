========
Cache
========

Redis Config
============

Example:

.. code-block:: shell
   :caption: conf.conf
   :name: conf.conf

   [cache]
   enabled = true
   backend = "dogpile.cache.redis"
   backend_argument = "url:redis://:aaaaaa@localhost:6379"
