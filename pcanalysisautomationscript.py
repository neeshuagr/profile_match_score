#!/usr/bin/python3.4

import fcntl
import time
import sys
import errno
import os
import utility
import config
import datetime


def automate_processes():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'pcanalysisautomationscript running')
    try:
        exec(open('download_crawldata_threading.py').read(), globals())
        exec(open('unzip_gz.py').read(), globals())
        exec(open('analyze_crawldata.py').read(), globals())
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += 'File: ' + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        # .encode('utf8'))
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


if __name__ == "__main__":
    f = open('lock', 'w')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write(...)
            sys.exit(-1)
        raise
    automate_processes()
