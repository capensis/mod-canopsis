.. _connectors_shinken2canopsis_setup:


Setup
=====

Requirements
------------

The broker module allow dynamic connection loss and reconnect with
canopsis amqp message bus without loss of events. You should consider to
set ``maxqueuelength`` (the maximum number of events that should be kept
in case of connection loss).

Run:

.. code-block:: bash

    easy_install kombu

Setup
-----

The canopsis broker module is natively present in shinken distribution.
You nead at least dev version.

You only need to enable the broker module and at least configure the
canopsis host address

Edit the ``etc/shinken-specific.cfg`` and add ``Canopsis`` to the list
of enabled modules

.. code-block:: python

    define broker {
      modules Livestatus, Simple-log, WebUI, Canopsis
    }

In the same file, search for the ``Canopsis`` module and at least set
the host directive to canopsis host address

.. code-block:: python

    define module{
           module_name          Canopsis
           module_type          canopsis
           host                 xxx.xxx.xxx.xxx
           port                 5672
           user                 guest
           password             guest
           virtual_host         canopsis
           exchange_name        canopsis.events
           identifier           shinken-1
           maxqueuelength       50000
           queue_dump_frequency 300
    }

When you want to plug Shinken on Canopsis there is a Mongodb port
conflict. So you have to change a little your Shinken configuration and
the mongodb.conf

Edit  ``/etc/mongodb.conf``

.. code-block:: bash

    port=27018

And restart it: 

.. code-block:: bash

    /etc/init.d/mongodb restart


Edit ``shinken-specific.cfg``

.. code-block:: python

    define module {
      module_name Mongodb
      module_type mongodb
      uri mongodb://localhost:27018/?safe=true
      database shinken
    }
    define module {
      module_name mongologs
      module_type logstore_mongodb
      mongodb_uri mongodb://localhost:27018/?safe=true
    }
    define module {
      module_name MongodbRetention
      module_type mongodb_retention
      uri mongodb://localhost:27018/?safe=true
      database shinken
    }
