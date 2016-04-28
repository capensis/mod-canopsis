.. _connectors_shinken2canopsis_use:


Use
===

Start / Stop
------------

As this connector is a nativ python module, **start** and **stop** actions depend on monitoring master process (Shinken).

So to start it 

.. code-block:: bash

    service shinken start

And to stop it 

.. code-block:: bash

    service shiken stop
