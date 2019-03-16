#!/usr/bin/python3.4

import fcntl
import time
import sys
import errno
import os


def automate_processes():
    exec(open('dataread.py').read(), globals())
    exec(open('nounphrase_generate.py').read(), globals())


if __name__ == "__main__":
    f = open('lock', 'w')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        automate_processes()
    except e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write(...)
            sys.exit(-1)
        raise
