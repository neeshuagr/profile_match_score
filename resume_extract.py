import config
import dbmanager
import utility
#import binascii
#import codecs
import io
#from mimetypes import guess_extension, guess_type
import custom
import datetime


def dbdata(connStr):
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'resume_extract running' + ' ' + str(datetime.datetime.now()))
    st_db_name_list = utility.find_string_inbetween(config.ConfigManager().STConnStr, "DATABASE=", ";UID")
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = custom.fetch_query(
        config.ConfigManager().STCandidateResumesQueryId)
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    query = query.replace('##candidateResumeID##', str(
        configdocs[0]['STcandidateResumepk'])).replace('##STDB##', st_db_name_list[0])
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
            resumefilepath = config.ConfigManager().ResumeDirectory + '/' + '_' + \
                str(data_dict['candidateID']) + '_' + \
                str(data_dict['documentName'])
            filewrite = open(resumefilepath, 'wb')
            f = io.BytesIO(data_dict['ResumeFile'])
            filewrite.write(data_dict['ResumeFile'])
            filefilepath = config.ConfigManager().fileDirectory + '/' + '_' + \
                str(data_dict['candidateResumeID'])+'-'+str(data_dict['supplierID']) + '_' + \
                str(data_dict['documentName'])
            file_write = open(filefilepath, 'wb')
            file_write.write(data_dict['ResumeFile'])
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)
    if 'candidateResumeID' in data_dict:
        primary_key = data_dict['candidateResumeID']
        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet = utility.clean_dict()
        UpdateTemplateSet['STcandidateResumepk'] = primary_key
        resumeUpdateDelta = int(primary_key) - \
            int(configdocs[0]['STcandidateResumepk'])
        utility.write_to_file(config.ConfigManager().LogFile,
                              'a', 'Resumes updated - ' + str(resumeUpdateDelta) + ' ' + str(datetime.datetime.now()))
        UpdateTemplateWhere['_id'] = configdocs[0]['_id']
        DBSet = utility.clean_dict()
        DBSet['$set'] = UpdateTemplateSet
        custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
        ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


dbdata(config.ConfigManager().STBinConnStr)
