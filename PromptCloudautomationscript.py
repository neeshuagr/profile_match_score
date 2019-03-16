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
    utility.write_to_file(config.ConfigManager().PromptcloudLogFile,
                          'a', 'PromptCloudautomationscript running')
    try:
        # download files into PCCompData with in mnt/nlpdata,xml format..
        exec(open('pc_download_crawldata_threading.py').read(), globals())
        # compress the PCCompdata folder
        exec(open('compress.py').read(), globals())
        # unzip files created in PCData folder time stored in dataloadconfig..
        exec(open('pc_unzip_gz.py').read(), globals())
        # download data into pcdataanalysisresults.ods
        exec(open('analyze_crawldata.py').read(), globals())
        # for automatically sending emails
        # exec(open('mailsend.py').read(), globals())
        # store analysis file in s3 backup
        exec(open('pcdataanalysisbackup.py').read(), globals())
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += 'File: ' + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        # .encode('utf8'))
        utility.write_to_file(
            config.ConfigManager().PromptcloudLogFile, 'a', exception_message)

if __name__ == "__main__":
    f = open('PClock', 'w')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'try block')
    except e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write(...)
            sys.exit(-1)
        utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'exception')
        raise
    automate_processes()
