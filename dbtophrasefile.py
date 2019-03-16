#!/usr/bin/python3.4
#   Symantic Graph Generation Module


import dcrconfig
import config
import sys
from pymongo import MongoClient
import utility
import datetime

utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
                      'dbtophrasefile.py running.. ' + str(datetime.datetime.now()))

# Master collection
cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().RatesDB]
datacol = db[config.ConfigManager().masterCollection]

# Config collection
configcol = db[config.ConfigManager().RatesConfigCollection]
configdocs = configcol.find({})
dbToPhraseDocId = configdocs[0]['dataDbToPhraseDocId']

jobDescPhrasesFile = open(dcrconfig.ConfigManager().PhraseFile, 'w')
jcount = 0
docIdList = []

# Non Smart Track record nounphrases created in phrases text file
for doc in datacol.find({"$and": [{"doc_id": {"$gt": dbToPhraseDocId}}, {"source": {"$ne": "Smart Track"}}]}).sort([("doc_id", 1)]).limit(20000):
    try:
        allphrases = ''
        phrases = doc['nounPhrases']
        docId = int(doc['doc_id'])
        docIdList.append(docId)
        jobUniqueId = '-' * 3 + str(docId) + '-' * 3
        allphrases += '\n' + jobUniqueId + '\n' + phrases
        print(allphrases, file=jobDescPhrasesFile)
        jcount += 1

        #  Print status
        print('.', end='')
        sys.stdout.flush()
    except BaseException as ex:
        utility.log_exception_file(ex, dcrconfig.ConfigManager().SemanticGraphLogFile)

# Updating maximum value in order to take delta in next cycle
if docIdList:
    configcol.update({"_id": configdocs[0]['_id']}, {"$set":
                     {"dataDbToPhraseDocId": max(docIdList)}})

print("total documents processed: " + str(jcount))
