import dcrconfig
import config
import sys
from pymongo import MongoClient
import utility
import datetime
import csv

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().RatesDB]
datacol = db[config.ConfigManager().PCRatesDataColl]
fileCounter = 0
writer = csv.writer(open('/mnt/nlpdata/csv/pcfile1.csv', 'w'))

# for doc in datacol.find({"payrate": {"$exists": True}}):
for doc in datacol.find({}, {'_id': 1, 'uniq_id': 1, 'jobtitle': 1, 'joblocation_address': 1, 'fileName': 1, 'payrate': 1}):

    _id = ' '
    uniq_id = ' '
    jobtitle = ' '
    joblocation_address = ' '
    fileName = ' '
    payrate = ' '
    fileCounter += 1
    if 'payrate' in doc:
        _id = ' '
        uniq_id = ' '
        jobtitle = ' '
        joblocation_address = ' '
        fileName = ' '
        payrate = ' '
        fileCounter += 1
    print('Record number: ' + str(fileCounter))
    if fileCounter == 1000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile2.csv', 'w'))
    if fileCounter == 2000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile3.csv', 'w'))
    if fileCounter == 3000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile4.csv', 'w'))
    if fileCounter == 4000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile5.csv', 'w'))
    if fileCounter == 5000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile6.csv', 'w'))
    if fileCounter == 6000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile7.csv', 'w'))
    if fileCounter == 7000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile8.csv', 'w'))
    if fileCounter == 8000001:
        writer = csv.writer(open('/mnt/nlpdata/csv/pcfile9.csv', 'w'))
    if '_id' in doc:
        _id = doc['_id']
    if 'uniq_id' in doc:
        uniq_id = doc['uniq_id']
    if 'jobtitle' in doc:
        jobtitle = doc['jobtitle']
    if 'joblocation_address' in doc:
        joblocation_address = doc['joblocation_address']
    if 'fileName' in doc:
        fileName = doc['fileName']
    if 'payrate' in doc:
        payrate = doc['payrate']

    writer.writerow([_id, uniq_id, jobtitle, joblocation_address, fileName, payrate])
