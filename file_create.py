import dcrconfig
import config
import sys
from pymongo import MongoClient
import utility
import datetime
import json
import dbmanager

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl['files_qa']
datacol = db['files']
# For all nounPhrases, records created in phrases text file
cursor = dbmanager.cursor_odbc_connection(config.ConfigManager().STBinConnStr)
batchId_list = []

# for doc in datacol.find({"isProcessed": 0}):
#     try:
#         filepath = config.ConfigManager().fileDirectory + '/' + '_' + str(doc['batchId']) + '_' + str(doc['fileName'])
#         filewrite = open(filepath, 'wb')
#         filewrite.write((doc['buffer']))
#     except BaseException as ex:
#         exception_message = '\n' + 'Exception:' + \
#            str(datetime.datetime.now()) + '\n'
#         exception_message += 'File: ' + '\n'
#         exception_message += '\n' + str(ex) + '\n'
#         exception_message += '-' * 100
#         utility.write_to_file(
#                 config.ConfigManager().LogFile, 'a', exception_message)


for doc in datacol.find({"isProcessed": 0}):
    try:
        batchId_list.append(doc['batchId'])
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
           str(datetime.datetime.now()) + '\n'
        exception_message += 'File: ' + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)


batchId_set = set(batchId_list)
unique_batchId_list = list(batchId_set)
query_where = ''
for item in unique_batchId_list:
    query_where += "'" + item + "'" + ','

query_where = query_where[:-1]

query = 'SELECT * FROM Files WHERE batchId IN' + '(' + query_where + ')'
db_data_dict = dbmanager.cursor_execute(cursor, query)
db_data = db_data_dict['dbdata']
db_data_cursorexec = db_data_dict['cursor_exec']
cursor_description = db_data_cursorexec.description
column_headers = [column[0] for column in cursor_description]

print(query)
for row in db_data:
    try:
        data_dict = dict(utility.zip_list(column_headers, row))
        filepath = config.ConfigManager().fileDirectory + '/' + '_' + str(data_dict['batchId']) + '_' + str(data_dict['documentName'])
        filewrite = open(filepath, 'wb')
        print(data_dict['fileData'])
        filewrite.write((data_dict['fileData']))
    except BaseException as ex:
        utility.log_exception(ex)


datacol.update({'isProcessed': 1}, {"$set": {"isProcessed": 0}})