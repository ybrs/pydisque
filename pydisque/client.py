"""Pydisque makes Disque easy to access in python."""

import redis
from redis.exceptions import ConnectionError
from functools import wraps
try:
  # Python 3
  from itertools import zip_longest
except ImportError:
  from itertools import izip_longest as zip_longest
import logging

logger = logging.getLogger(__name__)


class Job(object):

    """Represents a Disque Job."""

    def __init__(self, id, queue_name, payload):
        """Initialize a job."""
        self.id = id
        self.queue_name = queue_name
        self.payload = payload

    def __repr__(self):
        """Make a Job easy to read."""
        return '<Job id:%s queue_name:%s>' % (self.id, self.queue_name)


class Node(object):

    """Represents a Disque Node via host and port."""

    def __init__(self, node_id, host, port, connection):
        """
        Initialize a the Disque Node.

        :param node_id:
        :param host:
        :param port:
        :param connection: redis.Redis connection
        :returns:
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.connection = connection

    def __repr__(self):
        """Make Node easy to read."""
        return '<Node %s:%s>' % (self.host, self.port)


class retry(object):

    """retry utility object."""

    def __init__(self, retry_count=2):
        """Initialize retry utility object."""
        self.retry_count = retry_count

    def __call__(self, fn):
        """Function wrapper."""
        @wraps(fn)
        def wrapped_f(*args, **kwargs):
            c = 0
            while c <= self.retry_count:
                try:
                    return fn(*args, **kwargs)
                except:
                    logging.critical("retrying because of this exception - %s",
                                     c)
                    logging.exception("exception to retry ")
                    if c == self.retry_count:
                        raise
                c += 1

        return wrapped_f


class Client(object):

    """
    Client is the Disque Client.

    You can pass in a list of nodes, it will try to connect to
    first if it can't then it will try to connect to second and
    so forth.

    :Example:

    >>> client = Client(['localhost:7711', 'localhost:7712'])
    >>> client.connect()

    """

    def __init__(self, nodes=None):
        """Initalize a client to the specified nodes."""
        if nodes is None:
            nodes = ['localhost:7711']

        self.nodes = {}
        for n in nodes:
            self.nodes[n] = None

        self.connected_node = None

    def connect(self):
        """
        Connect to one of the Disque nodes.

        You can get current connection with connected_node property

        :returns: nothing
        """
        self.connected_node = None
        for i, node in self.nodes.items():
            host, port = i.split(':')
            port = int(port)
            redis_client = redis.Redis(host, port)
            try:
                ret = redis_client.execute_command('HELLO')
                format_version, node_id = ret[0], ret[1]
                others = ret[2:]
                self.nodes[i] = Node(node_id, host, port, redis_client)
                self.connected_node = self.nodes[i]
            except redis.exceptions.ConnectionError:
                pass
        if not self.connected_node:
            raise ConnectionError('couldnt connect to any nodes')
        logger.info("connected to node %s" % self.connected_node)

    def get_connection(self):
        """
        Return current connected_nodes connection.

        :rtype: redis.Redis
        """
        if self.connected_node:
            return self.connected_node.connection
        else:
            raise ConnectionError("not connected")

    @retry()
    def execute_command(self, *args, **kwargs):
        """Execute a command on the connected server."""
        try:
            return self.get_connection().execute_command(*args, **kwargs)
        except ConnectionError as e:
            logger.warn('trying to reconnect')
            self.connect()
            logger.warn('connected')
            raise

    def _grouper(self, iterable, n, fillvalue=None):
        """Collect data into fixed-length chunks or blocks."""
        args = [iter(iterable)] * n
        return zip_longest(fillvalue=fillvalue, *args)

    def info(self):
        """
        Return server information.

        INFO

        :returns: server info
        """
        return self.execute_command("INFO")

    def add_job(self, queue_name, job, timeout=200, replicate=None, delay=None,
                retry=None, ttl=None, maxlen=None, async=None):
        """
        Add a job to a queue.

        ADDJOB queue_name job <ms-timeout> [REPLICATE <count>] [DELAY <sec>]
            [RETRY <sec>] [TTL <sec>] [MAXLEN <count>] [ASYNC]

        :param queue_name: is the name of the queue, any string, basically.
        :param job: is a string representing the job.
        :param timeout: is the command timeout in milliseconds.
        :param replicate: count is the number of nodes the job should be
            replicated to.
        :param delay: sec is the number of seconds that should elapse
            before the job is queued by any server.
        :param retry: sec period after which, if no ACK is received, the
            job is put again into the queue for delivery. If RETRY is 0,
            the job has an at-most-once delivery semantics.
        :param ttl: sec is the max job life in seconds. After this time,
            the job is deleted even if it was not successfully delivered.
        :param maxlen: count specifies that if there are already count
            messages queued for the specified queue name, the message is
            refused and an error reported to the client.
        :param async: asks the server to let the command return ASAP and
            replicate the job to other nodes in the background. The job
            gets queued ASAP, while normally the job is put into the queue
            only when the client gets a positive reply.

        :returns: job_id
        """
        command = ['ADDJOB', queue_name, job, timeout]

        if replicate:
            command += ['REPLICATE', replicate]
        if delay:
            command += ['DELAY', delay]
        if retry is not None:
            command += ['RETRY', retry]
        if ttl:
            command += ['TTL', ttl]
        if maxlen:
            command += ['MAXLEN', maxlen]
        if async:
            command += ['ASYNC']

        # TODO(canardleteer): we need to handle "-PAUSE" messages more
        # appropriately, for now it's up to the person using the library
        # to handle a generic ResponseError on their own.
        logger.debug("sending job - %s", command)
        job_id = self.execute_command(*command)
        logger.debug("sent job - %s", command)
        logger.debug("job_id: %s " % job_id)
        return job_id

    def get_job(self, queues, timeout=None, count=None, nohang=False, withcounters=False):
        """
        Return some number of jobs from specified queues.

        GETJOB [NOHANG] [TIMEOUT <ms-timeout>] [COUNT <count>] [WITHCOUNTERS] FROM
            queue1 queue2 ... queueN

        :param queues: name of queues

        :returns: list of tuple(job_id, queue_name, job), tuple(job_id, queue_name, job, nacks, additional_deliveries) or empty list
        :rtype: list
        """
        assert queues

        command = ['GETJOB']
        if nohang:
            command += ['NOHANG']
        if timeout:
            command += ['TIMEOUT', timeout]
        if count:
            command += ['COUNT', count]
        if withcounters:
            command += ['WITHCOUNTERS']

        command += ['FROM'] + queues
        results = self.execute_command(*command)
        if not results:
            return []

        if withcounters:
            return [(job_id, queue_name, job, nacks, additional_deliveries) for
                    job_id, queue_name, job, _, nacks, _, additional_deliveries in results]
        else:
            return [(job_id, queue_name, job) for
                    job_id, queue_name, job in results]

    def ack_job(self, *job_ids):
        """
        Acknowledge the execution of one or more jobs via job IDs.

        ACKJOB jobid1 jobid2 ... jobidN

        :param job_ids: list of job_ids

        """
        self.execute_command('ACKJOB', *job_ids)

    def nack_job(self, *job_ids):
        """
        Acknowledge the failure of one or more jobs via job IDs.

        NACK jobid1 jobid2 ... jobidN

        :param job_ids: list of job_ids

        """
        self.execute_command('NACK', *job_ids)

    def fast_ack(self, *job_ids):
        """
        Perform a best effort cluster wide deletion of the specified job IDs.

        FASTACK jobid1 jobid2 ... jobidN

        :param job_ids:

        """
        self.execute_command('FASTACK', *job_ids)

    def working(self, job_id):
        """
        Signal Disque to postpone the next time it will deliver the job again.

        WORKING <jobid>

        :param job_id: name of the job still being worked on
        :returns: returns the number of seconds you (likely)
            postponed the message visiblity for other workers
        """
        return self.execute_command('WORKING', job_id)

    def qlen(self, queue_name):
        """
        Return the length of the named queue.

        QLEN <qname>

        :param queue_name: name of the queue
        :returns: length of the queue

        """
        return self.execute_command('QLEN', queue_name)

    # TODO (canardleteer): a QueueStatus object may be the best way to do this
    # TODO (canardleteer): return_dict should probably be True by default, but
    #                      i don't want to break anyones code
    def qstat(self, queue_name, return_dict=False):
        """
        Return the status of the queue (currently unimplemented).

        Future support / testing of QSTAT support in Disque

        QSTAT <qname>

        Return produced ... consumed ... idle ... sources [...] ctime ...
        """
        rtn = self.execute_command('QSTAT', queue_name)

        if return_dict:
            grouped = self._grouper(rtn, 2)
            rtn = dict((a, b) for a, b in grouped)

        return rtn

    def qpeek(self, queue_name, count):
        """
        Return, without consuming from queue, count jobs.

        If count is positive the specified number of jobs are
        returned from the oldest to the newest (in the same
        best-effort FIFO order as GETJOB). If count is negative
        the commands changes behavior and shows the count newest jobs,
        from the newest from the oldest.

        QPEEK <qname> <count>

        :param queue_name: name of the queue
        :param count:

        """
        return self.execute_command("QPEEK", queue_name, count)

    def enqueue(self, *job_ids):
        """
        Queue jobs if not already queued.

        :param job_ids:

        """
        return self.execute_command("ENQUEUE", *job_ids)

    def dequeue(self, *job_ids):
        """
        Remove the job from the queue.

        :param job_ids: list of job_ids

        """
        return self.execute_command("DEQUEUE", *job_ids)

    def del_job(self, *job_ids):
        """
        Completely delete a job from a node.

        Note that this is similar to FASTACK, but limited to a
        single node since no DELJOB cluster bus message is sent
        to other nodes.

        :param job_ids:

        """
        return self.execute_command("DELJOB", *job_ids)

    # TODO (canardleteer): a JobStatus object may be the best for this,
    #                      but I think SHOW is going to change to SHOWJOB
    def show(self, job_id, return_dict=False):
        """
        Describe the job.

        :param job_id:

        """
        rtn = self.execute_command('SHOW', job_id)

        if return_dict:
            grouped = self._grouper(rtn, 2)
            rtn = dict((a, b) for a, b in grouped)

        return rtn

    def pause(self, queue_name, kw_in=None, kw_out=None, kw_all=None,
              kw_none=None, kw_state=None, kw_bcast=None):
        """
        Pause a queue.

        Unfortunately, the PAUSE keywords are mostly reserved words in Python,
        so I've been a little creative in the function variable names. Open
        to suggestions to change it (canardleteer)

        :param queue_name: The job queue we are modifying.
        :param kw_in: pause the queue in input.
        :param kw_out: pause the queue in output.
        :param kw_all: pause the queue in input and output (same as specifying
                       both the in and out options).
        :param kw_none: clear the paused state in input and output.
        :param kw_state: just report the current queue state.
        :param kw_bcast: send a PAUSE command to all the reachable nodes of
                         the cluster to set the same queue in the other nodes
                         to the same state.
        """
        command = ["PAUSE", queue_name]
        if kw_in:
            command += ["in"]
        if kw_out:
            command += ["out"]
        if kw_all:
            command += ["all"]
        if kw_none:
            command += ["none"]
        if kw_state:
            command += ["state"]
        if kw_bcast:
            command += ["bcast"]

        return self.execute_command(*command)

    def qscan(self, cursor=0, count=None, busyloop=None, minlen=None,
              maxlen=None, importrate=None):
        """
        Iterate all the existing queues in the local node.

        :param count: An hint about how much work to do per iteration.
        :param busyloop: Block and return all the elements in a busy loop.
        :param minlen: Don't return elements with less than count jobs queued.
        :param maxlen: Don't return elements with more than count jobs queued.
        :param importrate: Only return elements with an job import rate
                        (from other nodes) >= rate.
        """
        command = ["QSCAN", cursor]
        if count:
            command += ["COUNT", count]
        if busyloop:
            command += ["BUSYLOOP"]
        if minlen:
            command += ["MINLEN", minlen]
        if maxlen:
            command += ["MAXLEN", maxlen]
        if importrate:
            command += ["IMPORTRATE", importrate]

        return self.execute_command(*command)

    def jscan(self, cursor=0, count=None, busyloop=None, queue=None,
              state=None, reply=None):
        """Iterate all the existing jobs in the local node.

        :param count: An hint about how much work to do per iteration.
        :param busyloop: Block and return all the elements in a busy loop.
        :param queue: Return only jobs in the specified queue.
        :param state: Must be a list - Return jobs in the specified state.
            Can be used multiple times for a logic OR.
        :param reply: None or string {"all", "id"} - Job reply type. Type can
            be all or id. Default is to report just the job ID. If all is
            specified the full job state is returned like for the SHOW command.
        """
        command = ["JSCAN", cursor]
        if count:
            command += ["COUNT", count]
        if busyloop:
            command += ["BUSYLOOP"]
        if queue:
            command += ["QUEUE", queue]
        if type(state) is list:
            for s in state:
                command += ["STATE", s]
        if reply:
            command += ["REPLY", reply]

        return self.execute_command(*command)

    def hello(self):
        """
        Returns hello format version, this node ID, all the nodes IDs, IP addresses, ports, and priority (lower is better, means node more available).
        Clients should use this as an handshake command when connecting with a Disque node.

        HELLO
        :returns: [<hello format version>, <this node ID>, [<all the nodes IDs, IP addresses, ports, and priority>, ...]
        """
        return self.execute_command("HELLO")




if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    c = Client(['localhost:7712', 'localhost:7711'])
    c.connect()
    import json
    job = json.dumps(["hello", "1234"])
    logger.info(c.add_job("test", job))

    jobs = c.get_job(['test'], timeout=5)
    for queue_name, job_id, payload in jobs:
        logger.info(job_id)
        c.ack_job(job_id)

    # while True:
    #     jobs = c.get_job(['test'], timeout=5)
