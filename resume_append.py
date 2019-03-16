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


def resume_append():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'resumeappend running' + ' ' + str(datetime.datetime.now()))
    DBWhereConditon = utility.clean_dict()
    DBWhereConditon['documentType'] = 'candidate details'
    DBWhereConditon['dataSource'] = 'Smart Track'
    DBWhereConditon['isResumeTextNew'] = 1
    DBProjection = utility.clean_dict()
    DBProjection['descriptionOld'] = 1
    DBProjection['resumeText'] = 1
    DBProjection['description'] = 1
    description = ''
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    descriptionUpdateCount = 0


    docs = custom.retrieve_rowdata_from_DB_projection(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().STCandidateCollection, DBWhereConditon, DBProjection)
    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    for doc in docs:
        try:
            if not 'descriptionAppend' in doc:
                if not doc['descriptionOld'] is None:
                    print('Inside if')
                    if 'resumeText' in doc:
                        description = doc['descriptionOld'] + \
                            '. ' + doc['resumeText']
                    else:
                        description = doc['descriptionOld']
                    UpdateTemplateSet = utility.clean_dict()
                    UpdateTemplateSet['description'] = description
                    UpdateTemplateSet['descriptionAppend'] = 1
                    descriptionUpdateCount += 1
                else:
                    print('Inside else')
                    description = doc['resumeText']
                    UpdateTemplateSet = utility.clean_dict()
                    UpdateTemplateSet['description'] = description
                    UpdateTemplateSet['descriptionAppend'] = 1
                    descriptionUpdateCount += 1
                UpdateTemplateSet['matchIndexProcess'] = 0
                UpdateTemplateWhere = utility.clean_dict()
                UpdateTemplateWhere['_id'] = doc['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().STCandidateCollection, UpdateTemplateWhere, DBSet, connection)
        except BaseException as ex:
            utility.log_exception(ex)

    utility.write_to_file(config.ConfigManager(
    ).LogFile, 'a', 'Number of descriptions updated - ' + str(descriptionUpdateCount) + ' ' + str(datetime.datetime.now()))

if __name__ == "__main__":
    resume_append()
