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
        self.client.add_job("test_q", json.dumps(["foo", str(t1)]),
                            timeout=100)
        jobs = self.client.get_job(['test_q'])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            job = json.loads(job)
            assert job[1] == str(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job(['test_q'], timeout=100)) == 0

    def test_nack(self):
        """
        Fetch the queue, return a job, check that it's back.
        """
        print self.client.info()
        t1 = time.time()
        self.client.add_job("test_nack_q", json.dumps(["foo", str(t1)]),
                            timeout=100)
        jobs = self.client.get_job(['test_nack_q'])
        # NACK the first read
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert len(jobs) == 1
            job = json.loads(job)
            assert job[1] == str(t1)
            self.client.nack_job(job_id)
        # this time ACK it
        jobs = self.client.get_job(['test_nack_q'])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            job = json.loads(job)
            assert job[1] == str(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job(['test_nack_q'], timeout=100)) == 0

    def test_qscan(self):
        """
            Kind of a simple test, just making sure we get something
            back.
        """
        t1 = time.time()
        qa = self.client.qscan()
        #print "Cursor: %s Jobs: %s" % (qa[0], qa[1])
        self.client.add_job("q1", str(t1), timeout=100)
        self.client.add_job("q2", str(t1), timeout=100)
        qb = self.client.qscan()
        #print "Cursor: %s Jobs: %s" % (qb[0], qb[1])
        assert qb[0]
        assert qb[1]

if __name__ == '__main__':
    unittest.main()
