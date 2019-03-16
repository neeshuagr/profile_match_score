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
    (dictionaries.DBWhereConditon)['documentType'] = 'candidate details'
    (dictionaries.DBWhereConditon)['dataSource'] = 'Smart Track'
    docs = custom.retrieve_rowdata_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.DBWhereConditon)
    description = ''
    for doc in docs:
        try:
            if not doc['descriptionOld'] is None:
                print('Inside if')
                description = doc['descriptionOld'] + '. ' + doc['resumeText']
                noun_phrases = dcrnlp.extract_nounphrases_sentences(
                    description)
                dictionaries.UpdateTemplateSet['nounPhrases'] = noun_phrases
                dictionaries.UpdateTemplateSet['description'] = description
            else:
                print('Inside else')
                description = doc['resumeText']
                noun_phrases = dcrnlp.extract_nounphrases_sentences(
                    description)
                dictionaries.UpdateTemplateSet['description'] = description
                dictionaries.UpdateTemplateSet['nounPhrases'] = noun_phrases
            dictionaries.UpdateTemplateWhere['_id'] = doc['_id']
            dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
            custom.update_data_to_Db(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
            ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet)
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
