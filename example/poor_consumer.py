"""poor_consumer is an example of mixing NACK and ACK messages."""
import json
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
    --nack=[float 0-1 | default 0.5] - percentage we should fail
    """
    print(txt)


def main():
    """Start the poor_consumer."""
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hn:v", ["help", "nack="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit()

    nack = 0.5
    verbose = False

    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-n", "--nack"):
            nack = float(a)
        else:
            assert False, "unhandled option"

    c = Client(['localhost:7712', 'localhost:7711'])
    c.connect()

    while True:
        jobs = c.get_job(['test'])
        for queue_name, job_id, job in jobs:
            rnd = random.random()
            job = json.loads(job)
            if rnd >= nack:
                print ">>> received job:", job
                c.ack_job(job_id)
            else:
                print ">>> bouncing job:", job
                c.nack_job(job_id)


if __name__ == "__main__":
    main()
