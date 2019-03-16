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
                          'a', ' master automationscript running')
    try:
        utility.update_config_coll_process_started_date()
        # Supplier master list load
        exec(open('st_master_supplier_data_read.py').read(), globals())
        # Client master list load
        exec(open('stclientsdataread.py').read(), globals())
        # Currency master list load
        exec(open('currencydataread.py').read(), globals())
        # Industry master list load
        exec(open('industrydataread.py').read(), globals())
        # MSP master list load
        exec(open('stmspdataread.py').read(), globals())
        # Rates information transfer from Smart Track
        exec(open('stratesdataread.py').read(), globals())
        # PromptCloud data load automation
        exec(open('prompt_cloud_automation.py').read(), globals())
        # Transferring files from staging collection to masters collection
        exec(open('staging_data_read.py').read(), globals())
        # Generating master integer graph
        exec(open('gen_docintgraph_from_db.py').read(), globals())
        # Transfering file to webserver
        exec(open('master_int_graph_transfer.py').read(), globals())
        # Learning automation
        exec(open('knowledge_build_automation.py').read(), globals())
    except BaseException as ex:
        utility.log_exception_file(config.ConfigManager().LogFile, ex)


if __name__ == "__main__":
    f = open('master_lock', 'w')
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
