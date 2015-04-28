import json
import time

from disque.client import Client

c = Client(['localhost:7712', 'localhost:7711'])
c.connect()

while True:
    print "sending job"
    c.add_job("test", json.dumps(["print", "hello", "world", time.time()]), timeout=100)
    time.sleep(1)