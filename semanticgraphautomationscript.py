#   !/usr/bin/python3.4
#   Data load from sql database (using odbc connection on sql server)


import dcrconfig
import utility
import fcntl
import time
import sys
import errno
import os
import datetime


def automate_processes():
    utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation start running..!' + str(datetime.datetime.now()))
    try:
        # Copies files from the previous cycle
        exec(open('filecopy.py').read(), globals())
        # download files into PCCompData with in mnt/nlpdata,xml format..
        exec(open('download_crawldata_threading.py').read(), globals())
        # unzip files created in PCData folder time stored in dataloadconfig..
        exec(open('unzip_gz.py').read(), globals())
        # collecting information from site..
        exec(open('pcdataload.py').read(), globals())
        # generate nounpharses for intelligence data..
        exec(open('nounphrase_gen_threading.py').read(), globals())
        # Copy the noun phrase text from Mongo DB (Intelligence collection)
        exec(open('dbtophrasefile.py').read(), globals())
        # Remove ngram anything above 3 or more words.
        exec(open('ngramremoval.py').read(), globals())
        # Remove duplicates and save it in new distinct phrase file.
        exec(open('duplicatefinder.py').read(), globals())
        # Checks if there is an existing semantic graph, if yes load and update
        # with new documents else create a new semantic graph and store.
        # Normally, this is run after n gram removal and duplicate
        # find and removal.
        exec(open('dcrgraphgenerator.py').read(), globals())
        # Read the semantic graph which is saved using dcrgraphgenerator.py
        # and read the document phrase file and create optimized integer
        # semantic edge file.
        exec(open('dcrgraphcompactor.py').read(), globals())
        # Save the node dictionary using pickle to file. This will be used by
        # above programs for finding node ids
        exec(open('savenodes.py').read(), globals())
        # Generate document integer graph and store. This will be used for
        # searching the documents.
        # exec(open('dcrdocumentintgraphgenerator.py').read(), globals())
        # Copy the noun phrase text from Mongo DB (Intelligence collection)
        exec(open('stdbtophrasefile.py').read(), globals())
        # Remove ngram anything above 3 or more words.
        exec(open('ngramremoval.py').read(), globals())
        # Remove duplicates and save it in new distinct phrase file.
        exec(open('duplicatefinder.py').read(), globals())
        # Checks if there is an existing semantic graph, if yes load and update
        # with new documents else create a new semantic graph and store.
        # Normally, this is run after n gram removal and duplicate
        # find and removal.
        exec(open('stdcrgraphgenerator.py').read(), globals())
        # Read the semantic graph which is saved using dcrgraphgenerator.py
        # and read the document phrase file and create optimized integer
        # semantic edge file.
        exec(open('stdcrgraphcompactor.py').read(), globals())
        # Save the node dictionary using pickle to file. This will be used by
        # above programs for finding node ids
        exec(open('savenodes.py').read(), globals())
        # Transfer generated intelligence files
        exec(open('filetransfer.py').read(), globals())
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += 'File: ' + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a', exception_message)


if __name__ == "__main__":
    f = open('SGlock', 'w')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a', 'try block')
    except e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write(...)
            sys.exit(-1)
        utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a', 'exception')
        raise
    automate_processes()
