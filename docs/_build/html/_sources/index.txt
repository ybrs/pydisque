.. pydisque documentation master file, created by
   sphinx-quickstart on Fri May  1 18:15:39 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pydisque's documentation!
====================================

Create a new Disque client by passing a list of nodes::

    from pydisque.client import Client
    client = Client(["127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"])
    client.connect()

If it can't connect to first node, it will try to connect to second, etc.., if it can't connect to any node, it will raise a redis.exceptions.ConnectionError as you can imagine.

Now you can add jobs::

    c.add_job("test_queue", json.dumps(["print", "hello", "world", time.time()]), timeout=100)

It will push the job "print" to the queue "test_queue" with a timeout of 100
ms, and return the id of the job if it was received and replicated
in time. If it can't reach the node - maybe it was shutdown etc. - it will retry to connect to another node in given node list, and then send the job. If there is no avail nodes in your node list, it will obviously raise a ConnectionError

Then, your workers will do something like this::

    while True:
        jobs = c.get_job(['test'])
        for queue_name, job_id, job in jobs:
            job = json.loads(job)
            print ">>> received job:", job
            c.ack_job(job_id)

Contents:

.. toctree::
   :maxdepth: 2

.. autoclass:: pydisque.client.Client
    :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

