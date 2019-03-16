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


def route_dataread(filepaths):
    data_read_count = int(utility.read_from_file(
        config.ConfigManager().ExecutioncountFile, 'r'))
    file_read_count = 0
    file_path_count = 0
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    docid_count = int(configdocs[0]['docid_count'])
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'dataread running')
    for filepath in filepaths:
        data_text = ''
        try:
            file_path_count += 1
            print('File number: ' + str(file_path_count))
            print('Processing file..' + filepath)
            if filepath[-4:].lower() == ".txt":
                data_text = datareadfiletypes.read_text_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".pdf":
                data_text = datareadfiletypes.read_pdf_text(
                    filepath, data_text)
            elif filepath[-5:].lower() == ".docx":
                data_text = datareadfiletypes.read_docx_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".doc":
                data_text = datareadfiletypes.read_doc_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".xls":
                # data_text = datareadfiletypes.read_excel_text(
                    # filepath, data_text)
                docid_count = custom.process_excel_rowdata(
                    filepath, docid_count)
            elif filepath[-5:].lower() == ".xlsx":
                # data_text = datareadfiletypes.read_excel_text(
                    # filepath, data_text)
                docid_count = custom.process_excel_rowdata(
                    filepath, docid_count)
            elif filepath[-4:].lower() == ".csv":
                data_text = datareadfiletypes.read_csv_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".odt":
                data_text = datareadfiletypes.read_odt_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".xml":
                docid_count = custom.process_xml_data(filepath, docid_count)
            if not data_text == '':
                docid_count += 1
                file_read_count += 1
                # dcrnlp.extract_nounphrases_sentences(data_text)
                noun_phrases = ''
                dictionaries.DataProperties['description'] = data_text
                dictionaries.DataProperties['nounPhrases'] = noun_phrases
                dictionaries.DataProperties[
                    'documentType'] = utility.filefolder_from_filepath(filepath)
                dictionaries.DataProperties[
                    'dataSource'] = config.ConfigManager().Misc  # config.ConfigManager().JobPortal
                dictionaries.DataProperties['doc_id'] = docid_count
                dictionaries.DataProperties[
                    'documentTitle'] = utility.filename_from_filepath(filepath)
                dictionaries.DataProperties['documentDesc'] = (
                    dictionaries.DataProperties['description'])[0:200]
                jsonfordatastore = custom.prepare_json_for_datastore(
                    dictionaries.DataProperties)
                jsonfordatastore_deserialized = utility.jsonstring_deserialize(
                    jsonfordatastore)
                custom.insert_data_to_DB(
                    jsonfordatastore_deserialized, connection)
                phrases_file_data = custom.prepare_phrases_file_data(
                    noun_phrases, data_read_count, file_read_count)
                utility.write_to_file(
                    config.ConfigManager().PhraseFile, 'a', phrases_file_data)
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + filepath + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)

    data_read_count += 1
    utility.write_to_file(config.ConfigManager(
    ).ExecutioncountFile, 'w', str(data_read_count))
    dictionaries.UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    dictionaries.UpdateTemplateSet['docid_count'] = docid_count
    dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
    ).ConfigCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet, connection)


if __name__ == "__main__":

    file_paths = []
    directory_list = []
    directory_list = utility.string_to_array(
        config.ConfigManager().DirectoryList, ',', directory_list)
    file_paths = filemanager.directory_iterate(directory_list)
    route_dataread(file_paths)
    # utility.archive_content(
    # file_paths, config.ConfigManager().ArchiveDirectory)
    #connection = dbmanager.mongoDB_connection(int(config.ConfigManager().MongoDBPort))
    # configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager(
    #).MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager().ConfigCollection)
    #docid_count = int(configdocs[0]['docid_count'])
    # docid_count = custom.data_from_DB(config.ConfigManager().STConnStr, config.ConfigManager(
    #).STJobQueryId, config.ConfigManager().JobDetails, config.ConfigManager().ST, docid_count)
    # docid_count = custom.data_from_DB(config.ConfigManager().STConnStr, config.ConfigManager(
    #).STCandidateQueryId, config.ConfigManager().CandidateDetails, config.ConfigManager().ST, docid_count)
    # docid_count = custom.data_from_DB(config.ConfigManager().XchangeConnStr, config.ConfigManager(
    #).XchangeJobQueryId, config.ConfigManager().JobDetails, config.ConfigManager().Xchange, docid_count)
    # docid_count = custom.data_from_DB(config.ConfigManager().XchangeConnStr, config.ConfigManager(
    #).XchangeCandidateQueryId, config.ConfigManager().CandidateDetails, config.ConfigManager().Xchange, docid_count)
    #dictionaries.UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    #dictionaries.UpdateTemplateSet['docid_count'] = docid_count
    #dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
    # custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
    # config.ConfigManager().ConfigCollection,
    # dictionaries.UpdateTemplateWhere, dictionaries.DBSet, connection)
