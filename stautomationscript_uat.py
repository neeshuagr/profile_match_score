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
                          'a', 'stautomationscript running')
    try:
        # Reading requirement and candidate data from ST
        # exec(open('stdataread.py').read(), globals())
        # Extracting candidate resumes
        # exec(open('resume_extract.py').read(), globals())
        # Read extracted resumes and update to 'resumeText' field
        # exec(open('resumeread.py').read(), globals())
        # Appending 'resumeText' to description field
        # exec(open('resume_append.py').read(), globals())
        # Generate nounphrases for candidate table
        # exec(open('stnounphrase_generate.py').read(), globals())
        # Update requirements and rates for candidates
        # exec(open('requirement_update_fastest.py').read(), globals())
        # Update candidate statuses which changed
        # exec(open('submission_status_update.py').read(), globals())
        # Extracting requirement description files
        # exec(open('req_desc_file_extract.py').read(), globals())
        # Read extracted description files and update to 'reqFileDesc' field
        # exec(open('req_desc_file_read.py').read(), globals())
        # Appending 'reqFileDesc' to description field
        # exec(open('req_desc_file_append.py').read(), globals())
        # Generate nounPhrases for requirement tables
        # exec(open('streqnounphrase_generate.py').read(), globals())
        # Get supplier info
        # exec(open('stsupplierdataread.py').read(), globals())
        # Candidate resume screening
        # exec(open('contactinfodetect.py').read(), globals())
        # Client master list load
        exec(open('stclientsdataread.py').read(), globals())
        # Currency master list load
        exec(open('currencydataread.py').read(), globals())
        # Industry master list load
        exec(open('industrydataread.py').read(), globals())
        # MSP master list load
        exec(open('stmspdataread.py').read(), globals())
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
