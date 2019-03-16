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


def requirement_update():
    (dictionaries.DBWhereCondition1)['documentType'] = 'candidate details'
    (dictionaries.DBWhereCondition1)['dataSource'] = 'Smart Track'
    docs = custom.retrieve_rowdata_from_DB_notimeout(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.DBWhereCondition1)
    recordnumber = 0
    for doc in docs:
        recordnumber += 1
        requirementIDList = []
        query = custom.fetch_query(
            config.ConfigManager().STCandidateSubmissionsQueryId)
        cursor = dbmanager.cursor_odbc_connection(
            config.ConfigManager().STConnStr)
        db_data_dict = dbmanager.cursor_execute(cursor, query)
        db_data = db_data_dict['dbdata']
        db_data_cursorexec = db_data_dict['cursor_exec']
        cursor_description = db_data_cursorexec.description
        column_headers = [column[0] for column in cursor_description]
        for row in db_data:
            try:
                data_dict = dict(utility.zip_list(column_headers, row))
                if (data_dict['CandidateID'] == doc['candidateid']):
                    requirementIDList.append(data_dict['requirementID'])
            except BaseException as ex:
                exception_message = '\n' + 'Exception:' + \
                    str(datetime.datetime.now()) + '\n'
                exception_message += 'File: ' + '\n'
                exception_message += '\n' + str(ex) + '\n'
                exception_message += '-' * 100
                utility.write_to_file(
                    config.ConfigManager().LogFile, 'a', exception_message)
        dictionaries.UpdateTemplateSet['requirementIDList'] = requirementIDList
        dictionaries.UpdateTemplateWhere['candidateid'] = doc['candidateid']
        dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
        print(recordnumber, doc['candidateid'], requirementIDList)
        custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
        ).DataCollectionDB, config.ConfigManager().DataCollectionDBCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet)


if __name__ == "__main__":
    requirement_update()
