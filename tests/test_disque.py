"""Unit Tests for the pydisque module."""

import unittest
import json
import time
import random
import six
from pydisque.client import Client


class TestDisque(unittest.TestCase):

    """TestCase class for pydisque."""

    def setUp(self):
        """Setup the tests."""
        self.client = Client(['localhost:7711'])
        self.client.connect()

    def test_publish_and_receive(self):
        """Test the most important functions of pydisque."""
        t1 = str(time.time())
        self.client.add_job("test_q", t1, timeout=100)
        jobs = self.client.get_job(['test_q'])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert job == six.b(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job(['test_q'], timeout=100)) == 0

    def test_nack(self):
        """Fetch the queue, return a job, check that it's back."""
        t1 = str(time.time())
        self.client.add_job("test_nack_q", str(t1), timeout=100)
        jobs = self.client.get_job(['test_nack_q'])
        # NACK the first read
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert len(jobs) == 1
            assert job == six.b(t1)
            self.client.nack_job(job_id)
        # this time ACK it
        jobs = self.client.get_job(['test_nack_q'])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert job == six.b(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job(['test_nack_q'], timeout=100)) == 0

    def test_qscan(self):
        """
        Test the qscan function.

        This test relies on add_job() being functional, and
        the local disque not being a disque proxy to a mesh.
        """
        t1 = str(time.time())
        qa = self.client.qscan()
        # print "Cursor: %s Jobs: %s" % (qa[0], qa[1])
        self.client.add_job("q1", t1, timeout=100)
        self.client.add_job("q2", t1, timeout=100)
        qb = self.client.qscan()
        # print "Cursor: %s Jobs: %s" % (qb[0], qb[1])
        assert qb[0]
        assert qb[1]

        # i am thinking we need some kind of master 'clear queue'
        # command in disque, hopefully not just for the purposes of
        # making this unit test more effective...
        assert six.b("q1") in qb[1]
        assert six.b("q2") in qb[1]

    def test_jscan(self):
        """Simple test of the jscan function."""
        t1 = time.time()
        queuename = "test_jscan-%d" % random.randint(1000, 1000000)
        j1 = self.client.add_job(queuename, str(t1), timeout=100)
        jerbs = self.client.jscan(queue=queuename)
        assert j1 in jerbs[1]

if __name__ == '__main__':
    unittest.main()
