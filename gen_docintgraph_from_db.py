#!/usr/bin/python3.4
#   Document integer graph generation module


import dcrconfig
import config
import sys
from pymongo import MongoClient
import utility
import datetime
import dcrgraph

utility.write_to_file(config.ConfigManager().LogFile, 'a',
                      'Document integer graph from DB!(gen_docintgraph_from_db.py)' + str(datetime.datetime.now()))
cl = MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().mongoDBHost))
db = cl[config.ConfigManager().RatesDB]
mastercoll = db[config.ConfigManager().masterCollection]
ratesConfig = db[config.ConfigManager().RatesConfigCollection]

ratesConfigValues = ratesConfig.find({})
masterDateCreated = ratesConfigValues[0]['masterDateModified']
masterDateCreatedList = []


for doc in mastercoll.find({"dateCreated": {"$gt": masterDateCreated}}):
    try:
        if not doc["signGraph"] == "":
            dcrgraph.generate_document_integer_graph_fromdb(doc["signGraph"], doc['doc_id'],'a',config.ConfigManager().masterDocumentIntegerFile)
            masterDateCreatedList.append(doc["dateCreated"])
    except BaseException as ex:
        utility.log_exception_file(str(ex), config.ConfigManager().LogFile)

try:
    if masterDateCreatedList:
        masterDateCreatedLatest = max(masterDateCreatedList)
        ratesConfig.update({"_id": ratesConfigValues[0]['_id']}, {"$set": {"masterDateModified": masterDateCreatedLatest}})
except BaseException as ex:
    utility.log_exception_file(str(ex), config.ConfigManager().LogFile)
