import json
import time
import logging
logging.basicConfig(level=logging.DEBUG)

from pydisque.client import Client

c = Client(['localhost:7712', 'localhost:7711'])
c.connect()

while True:
    jobs = c.get_job(['test'])
    for queue_name, job_id, job in jobs:
        job = json.loads(job)
        print ">>> received job:", job
        c.ack_job(job_id)