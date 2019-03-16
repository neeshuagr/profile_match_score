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
                          'a', 'staging automationscript running')
    try:
        # industry data read
        exec(open('industrydataread.py').read(), globals())
        # currency data read
        exec(open('currencydataread.py').read(), globals())
        # ST msp users data read
        exec(open('stmspdataread.py').read(), globals())
        # ST clients data read
        exec(open('stclientsdataread.py').read(), globals())
        # data move staging to master
        exec(open('stagingdataread.py').read(), globals())
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
        utility.write_to_file(config.ConfigManager().LogFile, 'a', 'try block')
    except e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write(...)
            sys.exit(-1)
        utility.write_to_file(config.ConfigManager().LogFile, 'a', 'exception')
        raise
    automate_processes()
