#!/usr/bin/python3.4

import datareadfiletypes
import config
import filemanager
import utility
import dcrnlp
import custom
import datetime
import pyodbc
import dbmanager
import db_requirements_candidate
import json


def requirement_update():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'requpdatefastest running' + ' ' + str(datetime.datetime.now()))
    recordnumber = 0
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    query = custom.fetch_query(
        config.ConfigManager().STCandidateSubmissionsQueryId)
    if '.' in str(configdocs[0]['submissionsdateCreated']):
        query = query.replace('##dateCreated##', str(configdocs[0]['submissionsdateCreated']).split(
            '.')[0] + ('.') + str(configdocs[0]['submissionsdateCreated']).split('.')[1][:3])
    else:
        query = query.replace('##dateCreated##', str(
            configdocs[0]['submissionsdateCreated']))
    # query = query.replace('##dateCreated##', str((datetime.datetime.now() - datetime.timedelta(minutes=480))).split('.')[0] + ('.') + str((datetime.datetime.now() - datetime.timedelta(minutes=480))).split('.')[1][:3])
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
    for row1 in db_data:
        try:
            strtimestamp = str(datetime.datetime.now())
            recordnumber += 1
            print(recordnumber)
            data_dict1 = dict(utility.zip_list(column_headers, row1))
            if not (data_dict1['requirementID']).strip():
                requirementIDListperCandidate = []
                reqratelist = []
                reqratestatdictlist = []
            else:
                reqratelist = []
                reqratestatdictlist = []
                requirementIDListperCandidate = [reqID for reqID in (
                    data_dict1['requirementID']).split(',')]
                for reqratecombID in requirementIDListperCandidate:
                    reqratelistinit = [reqrateID for reqrateID in (
                        reqratecombID.strip().replace('~!@- ', '~!@-')).split('~!@-')]
                    print(reqratelistinit[0])
                    reqratestatdict = {}
                    req_list.append(int(reqratelistinit[1]))
                    if reqratelistinit[0] == "0.00":
                        reqratelistinit[0] = ""
                    reqratestatdict['rate'] = reqratelistinit[0]
                    reqratestatdict['requirementId'] = int(reqratelistinit[1])
                    reqratestatdict['candidateStatus'] = reqratelistinit[2]
                    reqratelist.append(reqratelistinit)
                    reqratestatdictlist.append(reqratestatdict)
            strtimestamp += ' ' + str(datetime.datetime.now())
            UpdateTemplateSet = utility.clean_dict()
            UpdateTemplateWhere = utility.clean_dict()
            UpdateTemplateSet['requirementIDList'] = reqratelist  # requirementIDListperCandidate
            UpdateTemplateSet['requirementRateStatusList'] = reqratestatdictlist
            UpdateTemplateWhere['candidateid'] = data_dict1['CandidateID']
            UpdateTemplateWhere['documentType'] = 'candidate details'
            UpdateTemplateWhere['dataSource'] = 'Smart Track'
            DBSet = utility.clean_dict()
            DBSet['$set'] = UpdateTemplateSet
            print(UpdateTemplateSet['requirementRateStatusList'])
            custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
            ).STCandidateCollection, UpdateTemplateWhere, DBSet, connection)
            strtimestamp += ' ' + str(datetime.datetime.now())
        except BaseException as ex:
            utility.log_exception(ex)
    if 'dateCreated' in data_dict1:
        if not data_dict1['dateCreated'] is None:
            UpdateTemplateSet = utility.clean_dict()
            UpdateTemplateWhere = utility.clean_dict()
            UpdateTemplateSet['submissionsdateCreated'] = data_dict1['dateCreated']
            UpdateTemplateWhere['_id'] = configdocs[0]['_id']
            DBSet = utility.clean_dict()
            DBSet['$set'] = UpdateTemplateSet
            custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
            ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)
    if req_list:
        print(list(set(req_list)))
        utility.write_to_file(config.ConfigManager().LogFile,'a', 'requirement id list - ' + str(json.dumps(list(set(req_list)))) + ' ' + str(datetime.datetime.now()))
        db_requirements_candidate.generate_req_candidate_file_selected_req(list(set(req_list)))
    utility.write_to_file(config.ConfigManager(
    ).LogFile, 'a', 'Number of records for which requirement list was updated - ' + str(recordnumber) + ' ' + str(datetime.datetime.now()))


if __name__ == "__main__":
    requirement_update()
