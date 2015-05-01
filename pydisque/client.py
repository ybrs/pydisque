import redis
from redis.exceptions import ConnectionError
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class Job(object):
    def __init__(self, id, queue_name, payload):
        self.id = id
        self.queue_name = queue_name
        self.payload = payload

    def __repr__(self):
        return '<Job id:%s queue_name:%s>' % (self.id, self.queue_name)

class Node(object):
    def __init__(self, node_id, host, port, connection):
        """

        :param node_id:
        :param host:
        :param port:
        :param connection: redis.Redis connection
        :return:
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.connection = connection

    def __repr__(self):
        return '<Node %s:%s>' % (self.host, self.port)

class retry(object):
    def __init__(self, retry_count=2):
        self.retry_count = retry_count

    def __call__(self, fn):

        @wraps(fn)
        def wrapped_f(*args, **kwargs):
            c = 0
            while c <= self.retry_count:
                try:
                    return fn(*args, **kwargs)
                except:
                    logging.critical("retrying because of this exception - %s", c)
                    logging.exception("exception to retry ")
                    if c == self.retry_count:
                        raise
                c += 1

        return wrapped_f

class Client(object):

    def __init__(self, nodes=None):
        """

        :param nodes: list of nodes that can be connected eg: ['localhost:7711', 'localhost:7712']

        """
        if nodes is None:
            nodes = ['localhost:7711']

        self.nodes = {}
        for n in nodes:
            self.nodes[n] = None

        self.connected_node = None

    def connect(self):
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
            raise Exception('couldnt connect to any nodes')
        logger.info("connected to node %s" % self.connected_node)

    def get_connection(self):
        """

        :rtype: redis.Redis
        """
        return self.connected_node.connection

    @retry()
    def execute_command(self, *args, **kwargs):
        try:
            return self.get_connection().execute_command(*args, **kwargs)
        except ConnectionError as e:
            logger.warn('trying to reconnect')
            self.connect()
            logger.warn('connected')
            raise

    def add_job(self, queue_name, job, timeout=200, replicate=None, delay=None,
                retry=None, ttl=None, maxlen=None, async=None):
        """
        ADDJOB queue_name job <ms-timeout> [REPLICATE <count>] [DELAY <sec>] [RETRY <sec>] [TTL <sec>] [MAXLEN <count>] [ASYNC]

        :param queue_name: is the name of the queue, any string, basically.
        :param job: is a string representing the job.
        :param timeout: is the command timeout in milliseconds.
        :param replicate: count is the number of nodes the job should be replicated to.
        :param delay: sec is the number of seconds that should elapse before the job is queued by any server.
        :param retry: sec period after which, if no ACK is received, the job is put again into the queue for delivery.
            If RETRY is 0, the job has an at-least-once delivery semantics.
        :param ttl: sec is the max job life in seconds. After this time, the job is deleted even if it was not successfully delivered.
        :param maxlen: count specifies that if there are already count messages queued for the specified queue name,
            the message is refused and an error reported to the client.
        :param async: asks the server to let the command return ASAP and replicate the job to other nodes in the background.
            The job gets queued ASAP, while normally the job is put into the queue only when the client gets a positive reply.

        :return:
        """
        command = ['ADDJOB', queue_name, job, timeout]

        if replicate:
            command += ['REPLICATE', replicate]
        if delay:
            command += ['DELAY', delay]
        if retry:
            command += ['RETRY', retry]
        if ttl:
            command += ['TTL', ttl]
        if maxlen:
            command += ['MAXLEN', maxlen]
        if async:
            command += ['ASYNC']

        logger.debug("sending job - %s", command)
        job_id = self.execute_command(*command)
        logger.debug("sent job - %s", command)
        logger.debug("job_id: %s " % job_id)
        return job_id

    def get_job(self, queues, timeout=None, count=None):
        """
        GETJOB [TIMEOUT <ms-timeout>] [COUNT <count>] FROM queue1 queue2 ... queueN

        :param queues: name of queues

        :return: list of (job_id, queue_name, payload) - or empty list
        :rtype: list
        """
        assert queues
        command = ['GETJOB']
        if timeout:
            command += ['TIMEOUT', timeout]
        if count:
            command += ['COUNT', count]

        command += ['FROM'] + queues
        results = self.execute_command(*command)
        if not results:
            return []
        return [(job_id, queue_name, payload) for job_id, queue_name, payload in results]

    def ack_job(self, *job_ids):
        """
        ACKJOB jobid1 jobid2 ... jobidN

        Acknowledges the execution of one or more jobs via job IDs.

        :param job_ids:
        :return:
        """
        self.execute_command('ACKJOB', *job_ids)

    def fast_ack(self, *job_ids):
        """
        FASTACK jobid1 jobid2 ... jobidN

        Performs a best effort cluster wide deletion of the specified job IDs.

        :param job_ids:
        :return:
        """
        self.execute_command('ACKJOB', *job_ids)

    def qlen(self, queue_name):
        """
        QLEN <qname>

        Length of queue

        :param queue:
        :return:
        """
        return self.execute_command('QLEN', queue_name)

    def qpeek(self, queue_name, count):
        """
        QPEEK <qname> <count>

        Return, without consuming from queue, count jobs.
        If count is positive the specified number of jobs are returned from the oldest
        to the newest (in the same best-effort FIFO order as GETJOB).
        If count is negative the commands changes behavior and shows the count newest jobs,
        from the newest from the oldest.

        :param queue_name:
        :param count:
        :return:
        """
        return self.execute_command("QPEEK", queue_name, count)

    def enqueue(self, *job_ids):
        """
        Queue jobs if not already queued.

        :param job_ids:
        :return:
        """
        return self.execute_command("ENQUEUE", *job_ids)

    def dequeue(self, *job_ids):
        """
        Remove the job from the queue.

        :param job_ids:
        :return:
        """
        return self.execute_command("DEQUEUE", *job_ids)

    def del_job(self, *job_ids):
        """
        Completely delete a job from a node. Note that this is similar to FASTACK,
        but limited to a single node since no DELJOB cluster bus message is sent to other nodes.

        :param job_ids:
        :return:
        """
        return self.execute_command("DELJOB", *job_ids)

    def show(self, job_id):
        """
        Describe the job.

        :param job_id:
        :return:
        """
        return self.execute_command("SHOW", job_id)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    c = Client(['localhost:7712', 'localhost:7711'])
    c.connect()
    import json
    job = json.dumps(["hello", "1234"])
    print c.add_job("test", job)

    jobs = c.get_job(['test'], timeout=5)
    for queue_name, job_id, payload in jobs:
        print job_id
        c.ack_job(job_id)

    # while True:
    #     jobs = c.get_job(['test'], timeout=5)