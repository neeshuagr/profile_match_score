#!/usr/bin/python3.4

import datareadfiletypes
import config
import filemanager
import utility
import dcrnlp
import custom
import datetime
import dictionaries
import pyodbc
import dbmanager


def nounphrase_generate():
    docs = custom.retrieve_rowdata_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.DBWhereConditon)
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    description = ''
    for doc in docs:
        try:
            description = doc['description']
            noun_phrases = dcrnlp.extract_nounphrases_sentences(description)
            dictionaries.UpdateTemplateSet['nounPhrases'] = noun_phrases
            dictionaries.UpdateTemplateWhere['_id'] = doc['_id']
            dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
            custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
            ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet, connection)
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)


if __name__ == "__main__":
    nounphrase_generate()
