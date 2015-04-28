pydisque
=========

Client for Disque, an in-memory, distributed job queue.

Usage
-----

Create a new Disque client by passing a list of nodes:

```python
from pydisque.client import Client
client = Client(["127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"])
client.connect()
```

Now you can add jobs:

```python
c.add_job("test_queue", json.dumps(["print", "hello", "world", time.time()]), timeout=100)
```

It will push the job "print" to the queue "test_queue" with a timeout of 100
ms, and return the id of the job if it was received and replicated
in time.

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

Documentation
------------
For now please check docstrings in disque/client.py

Installation
------------

You can install it using pip.

```
$ pip install pydisque
```