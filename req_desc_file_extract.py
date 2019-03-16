import config
import dbmanager
import utility
import io
import custom
import datetime


def dbdata(connStr):
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'req_desc_file_extract running' + ' ' + str(datetime.datetime.now()))
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = custom.fetch_query(
        config.ConfigManager().STReqDocQueryId)
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    query = query.replace('##jobDocumentID##', str(
        configdocs[0]['STreqDocpk']))
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    count = 0
    data_dict = {}
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            reqdescfilepath = config.ConfigManager().ReqDocDirectory + '/' + '_' + \
                str(data_dict['requirementID']) + '_' + \
                str(data_dict['documentName'])
            filewrite = open(reqdescfilepath, 'wb')
            filewrite.write(data_dict['jobDocumentFile'])
            count += 1
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)
    if 'jobDocumentID' in data_dict:
        primary_key = data_dict['jobDocumentID']
        UpdateTemplateSet = utility.clean_dict()
        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet['STreqDocpk'] = primary_key
        reqDocUpdateDelta = int(primary_key) - \
            int(configdocs[0]['STreqDocpk'])
        utility.write_to_file(config.ConfigManager().LogFile,
                              'a', 'Req docs updated - ' + str(count) + ' ' + str(datetime.datetime.now()))
        UpdateTemplateWhere['_id'] = configdocs[0]['_id']
        DBSet = utility.clean_dict()
        DBSet['$set'] = UpdateTemplateSet
        custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
        ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


dbdata(config.ConfigManager().STConnStr)
