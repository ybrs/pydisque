"""
Unit Tests for the pydisque module.

Currently, most of these tests require a fresh instance of
Disque to be valid and pass.
"""

import unittest
import json
import time
import random
import six
from six import b
from pydisque.client import Client
from redis.exceptions import ResponseError

class TestDisque(unittest.TestCase):

    """TestCase class for pydisque."""

    testID = None

    def setUp(self):
        """Setup the tests."""
        self.client = Client(['localhost:7711'])
        self.client.connect()
        self.testID = "%d.%d" % (time.time(),
                                 random.randint(1000, 1000000))

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
        queuename = "test_nack." + self.testID
        self.client.add_job(queuename, str(t1), timeout=100)
        jobs = self.client.get_job([queuename])
        # NACK the first read
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert len(jobs) == 1
            assert job == six.b(t1)
            self.client.nack_job(job_id)
        # this time ACK it
        jobs = self.client.get_job([queuename])
        assert len(jobs) == 1
        for queue_name, job_id, job in jobs:
            assert job == six.b(t1)
            self.client.ack_job(job_id)
        assert len(self.client.get_job([queuename], timeout=100)) == 0

    def test_qpeek(self):
        """
        Test qpeek.

        Ran into some problems with an ENQUEUE/DEQUEUE test that
        was using qpeek, checking core functionality of qpeek().
        """
        queuename = "test_qpeek-%s" % self.testID
        job_id = self.client.add_job(queuename, "Peek A Boo")

        peeked = self.client.qpeek(queuename, 1)
        assert peeked[0][1] == job_id

    def test_qscan(self):
        """
        Test the qscan function.

        This test relies on add_job() being functional, and
        the local disque not being a disque proxy to a mesh.

        TODO: unique the queues with self.testID.
        """
        t1 = str(time.time())

        self.client.add_job("q1", t1, timeout=100)
        self.client.add_job("q2", t1, timeout=100)

        qb = self.client.qscan()
        assert qb[0]
        assert qb[1]

        assert six.b("q1") in qb[1]
        assert six.b("q2") in qb[1]

    def test_jscan(self):
        """Simple test of the jscan function."""
        t1 = time.time()
        queuename = "test_jscan-%s" % self.testID
        j1 = self.client.add_job(queuename, str(t1), timeout=100)

        jerbs = self.client.jscan(queue=queuename)
        assert j1 in jerbs[1]

    def test_del_job(self):
        """Simple test of del_job, needs qpeek.

        FIXME: This function has grown ugly.
        """
        t1 = time.time()
        queuename = "test_del_job-%s" % self.testID

        j1 = self.client.add_job(queuename, str(t1))

        jerbs = self.client.qpeek(queuename, 1)
        jlist = []
        for item in jerbs:
            jlist.append(item[1])

        assert j1 in jlist

        self.client.del_job(j1)

        jerbs = self.client.qpeek(queuename, 1)
        jlist = []
        for item in jerbs:
            jlist.append(item[1])

        assert j1 not in jerbs

    def test_qlen(self):
        """Simple test of qlen."""
        queuename = "test_qlen-%s" % self.testID

        lengthOfTest = 100
        test_job = "Useless Job."

        for x in range(lengthOfTest):
            self.client.add_job(queuename, test_job)

        assert self.client.qlen(queuename) == lengthOfTest

    def test_qstat(self):
        """Testing QSTAT (default behavior)."""
        queuename = "test_qstat-%s" % self.testID

        testqueue = ["a", "b", "c"]
        for x in testqueue:
            self.client.add_job(queuename, x)

        stat = self.client.qstat(queuename)
        # check the basics
        assert b'jobs-in' in stat
        assert b'jobs-out' in stat

    def test_qstat_dict(self):
        """Testing QSTAT's (new dict behavior)."""
        queuename = "test_qstat_dict-%s" % self.testID

        testqueue = ["a", "b", "c"]
        for x in testqueue:
            self.client.add_job(queuename, x)

        stat = self.client.qstat(queuename, True)

        assert stat.get(b'jobs-in', None) is not None
        assert stat.get(b'jobs-out', None) is not None

    def test_shownack(self):
        """Test that NACK and SHOW work appropriately."""
        queuename = "test_show-%s" % self.testID

        test_job = "Show me."

        self.client.add_job(queuename, test_job)

        jobs = self.client.get_job([queuename])
        for queue_name, job_id, job in jobs:
            self.client.nack_job(job_id)

        shown = self.client.show(job_id, True)

        assert shown.get(b'body') == b(test_job)
        assert shown.get(b'nacks') == 1

    def _test_pause(self):
        """ TODO: """
        """Test that a PAUSE message is acknowledged."""
        queuename = "test_show-%s" % self.testID

        test_job = "Jerbs, they are a thing"

        self.client.pause(queuename, kw_in=True)

        try:
            job_id = self.client.add_job(queuename, test_job)
        except ResponseError:
            pass

        # can we add a job again?
        self.client.pause(queuename, kw_none=True)

        job_id = self.client.add_job(queuename, test_job)

        jobs = self.client.get_job([queuename])

        # TODO(canardleteer): add a test of PAUSE SHOW

    def test_get_job(self):
        queue_name = "test_get_job." + self.testID

        job = str(time.time())
        job_id = self.client.add_job(queue_name, job)

        expected = [(b(queue_name), job_id, b(job))]
        got = self.client.get_job([queue_name], withcounters=False)
        assert expected == got

    def test_get_job_withcounters(self):
        queue_name = "test_get_job." + self.testID

        job = str(time.time())
        job_id = self.client.add_job(queue_name, job)

        nacks = 0
        additional_deliveries = 0
        expected = [(b(queue_name), job_id, b(job), nacks, additional_deliveries)]
        got = self.client.get_job([queue_name], withcounters=True)
        assert expected == got

if __name__ == '__main__':
    unittest.main()
