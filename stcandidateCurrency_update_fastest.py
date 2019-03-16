#!/usr/bin/python3.4

import datareadfiletypes
import config
import filemanager
from pymongo import MongoClient
import utility
import custom
import dcrconfig
import datetime
import pyodbc
import dbmanager
import db_requirements_candidate
import json

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().DataCollectionDB]
STCandidateCollection = db[config.ConfigManager().STCandidateCollection]
print(STCandidateCollection)

configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager().ConfigCollection)
dateModified = configdocs[0]['STCandidateCurrencyCodeLastDate']
# query = custom.rates_data_from_DB(config.ConfigManager().STConnStr, config.ConfigManager(
#     ).STCandidateCurrencyQueryId, config.ConfigManager().STCandidateCurrencyDetails, config.ConfigManager().ST, dateModified)


def stcandidate_update():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'in stcandidates update currency Code running.!' + ' ' + str(datetime.datetime.now()))
    recordnumber = 0
    query = custom.fetch_query(config.ConfigManager().STCandidateCurrencyQueryId)
    print(query)
    query = custom.query_variable_replace(query, config.ConfigManager().STCandidateCurrencyDetails, config.ConfigManager().ST)
    print(query)
    cursor = dbmanager.cursor_odbc_connection(config.ConfigManager().STConnStr)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict1 = {}
    req_list = []
    candidateDatesList = []

    for row1 in db_data:
        try:
            print(data_dict1)
            strtimestamp = str(datetime.datetime.now())
            recordnumber += 1
            print(recordnumber)
            data_dict1 = dict(utility.zip_list(column_headers, row1))
            STCandidateCollection.update({"$and": [{"candidateid": data_dict1['candidateid']}, {"requirementRateStatusList": {"$elemMatch": {"requirementId": data_dict1['requirementid']}}}]}, {"$set": {"requirementRateStatusList.$.currencyCode": data_dict1['currencycode'], "requirementRateStatusList.$.SupplierCurrencyCode": data_dict1['SupplierCurrencyCode'], "requirementRateStatusList.$.supplierRegBillRateEX": str(data_dict1['supplierRegBillRateEX'])}})
            candidateDatesList.append(data_dict1['dateCreated'])
        except BaseException as ex:
            print(ex)
            utility.log_exception_file(ex, config.ConfigManager().LogFile)
    if 'dateCreated' in data_dict1:
        maxCandDate = max(candidateDatesList)
        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet = utility.clean_dict()
        UpdateTemplateWhere['_id'] = configdocs[0]['_id']
        print(maxCandDate)
        print(str(maxCandDate))
        UpdateTemplateSet['STCandidateCurrencyCodeLastDate'] = str(maxCandDate)
        DBSet = utility.clean_dict()
        DBSet['$set'] = UpdateTemplateSet
        custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                                          config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


if __name__ == "__main__":
    stcandidate_update()
