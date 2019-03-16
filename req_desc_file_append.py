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


def req_desc_file_append():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'req_desc_file_append running' + ' ' + str(datetime.datetime.now()))
    DBWhereConditon = utility.clean_dict()
    DBWhereConditon['documentType'] = 'job details'
    DBWhereConditon['dataSource'] = 'Smart Track'
    DBWhereConditon['isReqFileDescNew'] = 1
    DBProjection = utility.clean_dict()
    DBProjection['descriptionOld'] = 1
    DBProjection['reqFileDesc'] = 1
    DBProjection['description'] = 1
    description = ''
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    descriptionUpdateCount = 0

    docs = custom.retrieve_rowdata_from_DB_projection(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().STReqCollection, DBWhereConditon, DBProjection)
    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()

    for doc in docs:
        try:
            if not 'descriptionAppend' in doc:
                if not doc['descriptionOld'] is None:
                    print('Inside if')
                    if 'reqFileDesc' in doc:
                        description = doc['descriptionOld'] + \
                            '. ' + doc['reqFileDesc']
                    else:
                        description = doc['descriptionOld']
                    UpdateTemplateSet = utility.clean_dict()
                    UpdateTemplateSet['description'] = description
                    UpdateTemplateSet['descriptionAppend'] = 1
                    descriptionUpdateCount += 1
                else:
                    print('Inside else')
                    description = doc['reqFileDesc']
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
                ).DataCollectionDB, config.ConfigManager().STReqCollection, UpdateTemplateWhere, DBSet, connection)
        except BaseException as ex:
            utility.log_exception(ex)

    utility.write_to_file(config.ConfigManager(
    ).LogFile, 'a', 'Number of descriptions updated - ' + str(descriptionUpdateCount) + ' ' + str(datetime.datetime.now()))

if __name__ == "__main__":
    req_desc_file_append()
