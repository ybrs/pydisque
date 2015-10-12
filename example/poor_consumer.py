"""poor_consumer is an example of mixing NACK and ACK messages."""
import time
import sys
import getopt
import random
import logging
logging.basicConfig(level=logging.DEBUG)

from pydisque.client import Client


def usage():
    """Print a command line helper."""
    txt = """
    poor_consumer.py [args]

    acceptable arguments:
    --nack=[float 0-1 | default 0.0] - percentage we should fail
    --servers=[comma separated host:port list
        defaults to: localhost:7712,localhost:7711]
        (can be a single host)
    --queues=[comma separated list of queues | default "test"]
    """
    print(txt)


def main():
    """Start the poor_consumer."""
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:v", ["help", "nack=",
                                   "servers=", "queues="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit()

    # defaults
    nack = 0.0
    verbose = False
    servers = "localhost:7712,localhost:7711"
    queues = "test"

    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("--nack"):
            nack = float(a)
        elif o in ("--servers"):
            servers = a
        elif o in ("--queues"):
            queues = a
        else:
            assert False, "unhandled option"

    # prepare servers and queus for pydisque
    servers = servers.split(",")
    queues = queues.split(",")

    c = Client(servers)
    c.connect()

    while True:
        jobs = c.get_job(queues)
        for queue_name, job_id, job in jobs:
            rnd = random.random()

            # as this is a test processor, we don't do any validation on
            # the actual job body, so lets just pay attention to id's

            if rnd >= nack:
                print ">>> received job:", job_id
                c.ack_job(job_id)
            else:
                print ">>> bouncing job:", job_id
                c.nack_job(job_id)


if __name__ == "__main__":
    main()
