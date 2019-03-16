#!/usr/bin/python3.4
import config
import dcrconfig
import utility
import dcrnlp
import custom
import datetime
import dictionaries
import dbmanager
from pymongo import MongoClient
from queue import Queue
from threading import Thread
import sys

count = 1
q = Queue()


def nounphrase_generate():
    c = MongoClient(dcrconfig.ConfigManager().Datadb)
    db = c[config.ConfigManager().IntelligenceDb]
    col = db[config.ConfigManager().IntelligenceDataCollection]
    docs = col.find({'nounPhrases': ""},
                    {"description": 1, "doc_id": 1, "_id": 1})

    mongoport = int(config.ConfigManager().MongoDBPort)
    connection = dbmanager.mongoDB_connection(mongoport)

    for doc in docs:
        try:
            data = {}
            data['desc'] = doc['description']
            data['_id'] = doc['_id']
            data['doc_id'] = doc['doc_id']
            data['connection'] = connection
            q.put(data)

        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + '\n'
            str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
                                  exception_message)


def generate_nounphrase_insert_into_db(data):
    global count
    try:
        status = "{:<8}".format(str(count)) + " :"
        status += str(datetime.datetime.now())
        count += 1
        mongoport = int(config.ConfigManager().MongoDBPort)
        col = config.ConfigManager().IntelligenceDataCollection
        desc = data['desc']

        noun_phrases = dcrnlp.extract_nounphrases_sentences(desc)

        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet = utility.clean_dict()
        DBSet = utility.clean_dict()
        UpdateTemplateWhere['_id'] = data['_id']
        UpdateTemplateSet['nounPhrases'] = noun_phrases
        UpdateTemplateSet['description'] = desc
        DBSet['$set'] = UpdateTemplateSet

        status += " |" + str(datetime.datetime.now())
        custom.update_data_to_Db_con(mongoport,
                                     config.ConfigManager().IntelligenceDb,
                                     col,
                                     UpdateTemplateWhere,
                                     DBSet,
                                     data['connection'])

        status += " |" + str(datetime.datetime.now())
        status += " :" + "{:<9}".format(str(data['doc_id']))
        print(status)

    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + '\n'
        str(datetime.datetime.now()) + '\n'
        exception_message += 'File: ' + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
                              exception_message)


def worker():
    while True:
        item = q.get()
        generate_nounphrase_insert_into_db(item)
        q.task_done()

if __name__ == "__main__":
    # nounphrase_generate()
    for i in range(5):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    nounphrase_generate()
    q.join()
