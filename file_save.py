import dcrconfig
import config
import sys
from pymongo import MongoClient
import utility
import datetime
import json
import dbmanager
import pyodbc

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl['files_qa']
datacol = db['files']
file_dict_list = []
query = 'insert into Files (batchId, documentName, fileData) values '
cnxn = pyodbc.connect(config.ConfigManager().STBinConnStr)
cursor = cnxn.cursor()
print(cursor)
print(cnxn)

for doc in datacol.find({"isProcessed": 0}):
    query += '(' + "'" + doc['batchId'] + "'" + ',' + "'" + doc['fileName'] + "'" + ',' + 'CONVERT(VARBINARY(MAX),' + "'" + doc['fileData'] + "'" + ')' + ')' + ','
query = query[:-1]
    #print(query)

cursor.execute(query)
cnxn.commit()
