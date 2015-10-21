pydisque
=========

Client for Disque, an in-memory, distributed job queue.

[![Build Status](https://travis-ci.org/ybrs/pydisque.png)](https://travis-ci.org/ybrs/pydisque.png)

Documentation
-------------

[Read The Docs](http://pydisque.readthedocs.org/en/latest/)

Usage
-----

Create a new Disque client by passing a list of nodes:

```python
from pydisque.client import Client
client = Client(["127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"])
client.connect()
```

If it can't connect to first node, it will try to connect to second, etc.., if it can't connect to any node, it will raise a redis.exceptions.ConnectionError as you can imagine.

Now you can add jobs:

```python
c.add_job("test_queue", json.dumps(["print", "hello", "world", time.time()]), timeout=100)
```

It will push the job "print" to the queue "test_queue" with a timeout of 100
ms, and return the id of the job if it was received and replicated
in time. If it can't reach the node - maybe it was shutdown etc. - it will retry to connect to another node in given node list, and then send the job. If there is no avail nodes in your node list, it will obviously raise a ConnectionError

Then, your workers will do something like this:

```python
while True:
    jobs = c.get_job(['test'])
    for queue_name, job_id, job in jobs:
        job = json.loads(job)
        print ">>> received job:", job
        c.ack_job(job_id)
```

also check examples directory.

While waiting jobs your connected node may go down, pydisque will try to connect to next node, so you can restart your nodes without taking down your clients.

Documentation
------------
For now please check docstrings in disque/client.py, implemented commands are

- info
- add_job
- get_job
- ack_job
- nack_job
- fast_ack
- working
- qlen
- qstat
- qpeek
- qscan
- jscan
- enqueue
- dequeue
- del_job
- show

Installation
------------

You can install it using pip.

```
$ pip install pydisque
```

License
-----------
This project is licensed under the terms of the MIT license

Credits
-----------
- [ybrs](https://github.com/ybrs)
- [lovelle](https://github.com/lovelle)
- [canardleteer](https://github.com/canardleteer)
