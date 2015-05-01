import json
import time
import logging
logging.basicConfig(level=logging.DEBUG)

from pydisque.client import Client

c = Client(['localhost:7712', 'localhost:7711'])
c.connect()

while True:
    t = time.time()
    print "sending job", t
    c.add_job("test", json.dumps(["print", "hello", "world", t]), replicate=1, timeout=100)
    time.sleep(2)