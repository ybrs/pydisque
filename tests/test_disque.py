import unittest
import json
import time
from pydisque.client import Client

class TestDisque(unittest.TestCase):

    def setUp(self):
        self.client = Client(['localhost:7711'])
        self.client.connect()

    def test_publish_and_receive(self):
        t1 = time.time()
        self.client.add_job("test_q", json.dumps(["foo", str(t1)]), timeout=100)
        jobs = self.client.get_job(['test_q'])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            job = json.loads(job)
            assert job[1] == str(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job(['test_q'], timeout=100)) == 0

if __name__ == '__main__':
    unittest.main()