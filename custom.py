import config
import utility
import xml.etree.ElementTree as ET
import dbmanager
import json
import dictionaries
import datetime
import datareadfiletypes
import dcrnlp
import dictionaries
from xlrd import open_workbook
from decimal import *
from pymongo import MongoClient
from bson.objectid import ObjectId
import sys


def prepare_phrases_file_data(noun_phrases, data_read_count, file_read_count):
    phrases_file_data = ''
    phrase_unique_id = '-' * 40 + \
        str(data_read_count) + '_' + str(file_read_count) + '-' * 40
    phrases_file_data += '\n' + phrase_unique_id + '\n'
    phrases_file_data += noun_phrases
    return phrases_file_data


def process_xml_data(filepath, docid_count):
    tree = datareadfiletypes.read_xml_tree(filepath)
    for page in tree.getroot().findall('page'):
        for jobinfo in page.findall('job_info'):
            try:
                dict_object = utility.xml_to_dict(ET.tostring(jobinfo))
                if 'job_description' in (dict_object['job_info']):
                    if not (dict_object['job_info'])['job_description'] == '':
                        docid_count += 1
                        description = prepare_description_for_xml(dict_object)
                        noun_phrases = ''  # dcrnlp.extract_nounphrases_sentences(
                        # description)
                        dictionaries.DataProperties[
                            'nounPhrases'] = noun_phrases
                        dictionaries.DataProperties[
                            'documentType'] = utility.filefolder_from_filepath(filepath)
                        dictionaries.DataProperties[
                            'dataSource'] = config.ConfigManager().JobPortal
                        dictionaries.DataProperties['doc_id'] = docid_count
                        jsonstring = prepare_json_for_xmldatastore(
                            dict_object, dictionaries.DataProperties)
                        jsonstring_parsed = utility.jsonstring_deserialize(
                            jsonstring)
                        insert_data_to_DB(jsonstring_parsed['job_info'])

                        print('Processing xml job info')
            except BaseException as ex:
                exception_message = '\n' + 'Exception:' +\
                    str(datetime.datetime.now()) + '\n'
                exception_message += 'File: ' + filepath + '\n'
                exception_message += '\n' + str(ex) + '\n'
                exception_message += '-' * 100
                utility.write_to_file(
                    config.ConfigManager().LogFile, 'a', exception_message)
    return docid_count


def process_excel_rowdata(filepath, docid_count):
    excel_file = open_workbook(filepath)
    for sheet in excel_file.sheets():
        header_keys = [sheet.cell(
            0, colindex).value for colindex in range(sheet.ncols)]
        for rowindex in range(1, sheet.nrows):
            try:
                docid_count += 1
                print(docid_count)
                row_dict = {header_keys[colindex]: sheet.cell(
                    rowindex, colindex).value for colindex in range(sheet.ncols)}
                description = prepare_description_for_excel(row_dict)
                noun_phrases = ''  # dcrnlp.extract_nounphrases_sentences(
                # description)
                dictionaries.DataProperties['description'] = description
                dictionaries.DataProperties['nounPhrases'] = noun_phrases
                dictionaries.DataProperties[
                    'documentType'] = utility.filefolder_from_filepath(filepath)
                dictionaries.DataProperties[
                    'dataSource'] = config.ConfigManager().Misc
                dictionaries.DataProperties['doc_id'] = docid_count
                row_dict = prepare_dict_for_exceldatastore(
                    row_dict, dictionaries.DataProperties)
                insert_data_to_DB(row_dict)
            except BaseException as ex:
                exception_message = '\n' + 'Exception:' +\
                    str(datetime.datetime.now()) + '\n'
                exception_message += 'File: ' + filepath + '\n'
                exception_message += '\n' + str(ex) + '\n'
                exception_message += '-' * 100
                utility.write_to_file(
                    config.ConfigManager().LogFile, 'a', exception_message)
    return docid_count


def insert_data_to_DB(data, connection):
    try:
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().DataCollectionDB)
        collection = dbmanager.retrieve_collection(
            data_base, config.ConfigManager().DataCollectionDBCollection)
        dbmanager.crud_create(
            data_base, config.ConfigManager().DataCollectionDBCollection, data)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def insert_data_to_DB_collection(data, collection, connection):
    try:
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().DataCollectionDB)
        dbmanager.crud_create(data_base, collection, data)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def insert_data_to_DB_dBCollection(data, collection, connection, Db):
    try:
        data_base = dbmanager.db_connection(
            connection, Db)
        dbmanager.crud_create(data_base, collection, data)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def insert_STdata_to_DB(data, connection, documentType):
    try:
        if documentType == config.ConfigManager().JobDetails:
            collectionname = config.ConfigManager().STReqCollection
        if documentType == config.ConfigManager().STSupplierDetails:
            collectionname = config.ConfigManager().STSupplierCollection
        if documentType == config.ConfigManager().CandidateDetails:
            collectionname = config.ConfigManager().STCandidateCollection
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().DataCollectionDB)
        dbmanager.crud_create(
            data_base, collectionname, data)
    except BaseException as ex:
        utility.log_exception(ex)


def insert_STdata_to_RatesDB(data, connection, documentType):
    try:
        if documentType == config.ConfigManager().STClientDetails:
            collectionname = config.ConfigManager().STClientsColl
        if documentType == config.ConfigManager().STMSPDetails:
            collectionname = config.ConfigManager().STMSPColl
        if documentType == config.ConfigManager().STSupplierDetails:
            collectionname = config.ConfigManager().STSupplierCollection
        if documentType == config.ConfigManager().STlaborCatagoryDetails:
            collectionname = config.ConfigManager().STlaborCateColl
        if documentType == config.ConfigManager().STTypeofServiceDetails:
            collectionname = config.ConfigManager().STtypeofServiceColl
        if documentType == config.ConfigManager().currencyDetails:
            collectionname = config.ConfigManager().currecyColl
        if documentType == config.ConfigManager().industryDetails:
            collectionname = config.ConfigManager().ratesIndustryColl
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().RatesDB)
        dbmanager.crud_create(
            data_base, collectionname, data)
    except BaseException as ex:
        utility.log_exception(ex)


def insert_xCHANGEdata_to_DB(data, connection, documentType):
    try:
        if documentType == config.ConfigManager().JobDetails:
            collectionname = config.ConfigManager().xCHANGEReqCollection
        if documentType == config.ConfigManager().CandidateDetails:
            collectionname = config.ConfigManager().xCHANGECandidateCollection
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().DataCollectionDB)
        dbmanager.crud_create(
            data_base, collectionname, data)
    except BaseException as ex:
        utility.log_exception(ex)


def update_STdata_to_DB(data_dict, connection, documentType):
    try:
        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet = utility.clean_dict()
        UpdateTemplateSet['description'] = data_dict['description']
        UpdateTemplateSet['documentTitle'] = data_dict['documentTitle']
        UpdateTemplateSet['documentDesc'] = data_dict['documentDesc']
        UpdateTemplateSet['descriptionOld'] = data_dict['description']
        UpdateTemplateSet['matchIndexProcess'] = 0
        DBSet = utility.clean_dict()
        DBSet['$set'] = UpdateTemplateSet
        if documentType == config.ConfigManager().JobDetails:
            UpdateTemplateWhere['requirementid'] = data_dict['requirementid']
            update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
            ).STReqCollection, UpdateTemplateWhere, DBSet, connection)
        if documentType == config.ConfigManager().CandidateDetails:
            UpdateTemplateWhere['candidateid'] = data_dict['candidateID']
            update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
            ).STCandidateCollection, UpdateTemplateWhere, DBSet, connection)
    except BaseException as ex:
        utility.log_exception(ex)


def update_xCHANGEdata_to_DB(data_dict, connection, documentType):
    try:
        UpdateTemplateWhere = utility.clean_dict()
        UpdateTemplateSet = utility.clean_dict()
        UpdateTemplateSet['description'] = data_dict['description']
        UpdateTemplateSet['documentTitle'] = data_dict['documentTitle']
        UpdateTemplateSet['documentDesc'] = data_dict['documentDesc']
        UpdateTemplateSet['descriptionOld'] = data_dict['description']
        UpdateTemplateSet['matchIndexProcess'] = 0
        DBSet = utility.clean_dict()
        DBSet['$set'] = UpdateTemplateSet
        # if documentType == config.ConfigManager().JobDetails:
            # update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
            # ).xCHANGEReqCollection, UpdateTemplateWhere, DBSet, connection)
        if documentType == config.ConfigManager().CandidateDetails:
            UpdateTemplateWhere['applicationuserid'] = data_dict['applicationuserid']
            update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
            ).xCHANGECandidateCollection, UpdateTemplateWhere, DBSet, connection)
    except BaseException as ex:
        utility.log_exception(ex)


def retrieve_data_from_DB(Port, DB, Col):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        documents = dbmanager.crud_read(data_base, collection)
        # print("Connection",connection,"data base", data_base,"collection", collection,"documents",documents)
        return documents
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def update_data_to_Db(Port, DB, Col, updatewhere, updateset):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        dbmanager.crud_update(collection, updatewhere, updateset)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def update_data_to_Db_con(Port, DB, Col, updatewhere, updateset, connection):
    try:
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        dbmanager.crud_update(collection, updatewhere, updateset)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def update_data_to_Db_noupsert(Port, DB, Col, updatewhere, updateset, connection):
    try:
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        dbmanager.crud_update_noupsert(collection, updatewhere, updateset)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def update_data_to_Db_noupsertsingle(Port, DB, Col, updatewhere, updateset, connection):
    try:
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        dbmanager.crud_update_noupsertsingle(
            collection, updatewhere, updateset)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def retrieve_rowdata_from_DB(Port, DB, Col, condition):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        document = dbmanager.crud_read_one(data_base, collection, condition)
        return document
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def retrieve_rowdata_from_DB_notimeout(Port, DB, Col, condition):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        document = dbmanager.crud_read_one_notimeout(
            data_base, collection, condition)
        return document
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def retrieve_rowdata_from_DB_projection(Port, DB, Col, condition, projection):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        document = dbmanager.crud_read_one_projection(data_base, collection, condition, projection)
        return document
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def retrieve_rowdata_from_DB_notimeout_projection(Port, DB, Col, condition, projection):
    try:
        connection = dbmanager.mongoDB_connection(int(Port))
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        document = dbmanager.crud_read_one_notimeout_projection(
            data_base, collection, condition, projection)
        return document
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def prepare_json_for_datastore(DataProperties):
    datastoretemplate = dictionaries.DataStoreTemplate
    datastoretemplate['description'] = DataProperties['description']
    datastoretemplate['nounPhrases'] = DataProperties['nounPhrases']
    datastoretemplate['documentType'] = DataProperties['documentType']
    datastoretemplate['dataSource'] = DataProperties['dataSource']
    datastoretemplate['doc_id'] = DataProperties['doc_id']
    datastoretemplate['documentTitle'] = DataProperties['documentTitle']
    datastoretemplate['documentDesc'] = DataProperties['documentDesc']
    jsonfordatastore = utility.dict_to_json(datastoretemplate)
    return jsonfordatastore


def prepare_description_for_xml(dict_object):
    description = (dict_object['job_info'])['job_description']
    if 'job_title'in (dict_object['job_info']):
        description += '. ' + (dict_object['job_info'])['job_title']
    if 'skills' in (dict_object['job_info']):
        description += '. ' + (dict_object['job_info'])['skills']
    return description


def prepare_description_for_excel(row_dict):
    description = ''
    if 'Job Title' in row_dict:
        if not row_dict['Job Title'] is None:
            description += row_dict['Job Title']
    if 'Job Description' in row_dict:
        if not row_dict['Job Description'] is None:
            description += row_dict['Job Description']
    if 'Team or Role' in row_dict:
        if not row_dict['Team or Role'] is None:
            description += row_dict['Team or Role']
    if 'Minimum or Required Skills (Comma separated)' in row_dict:
        if not row_dict['Minimum or Required Skills (Comma separated)'] is None:
            description += row_dict[
                'Minimum or Required Skills (Comma separated)']
    if 'Preferred Skills (Comma separated)' in row_dict:
        if not row_dict['Preferred Skills (Comma separated)'] is None:
            description += row_dict['Preferred Skills (Comma separated)']
    if 'Job Code/Title' in row_dict:
        if not row_dict['Job Code/Title'] is None:
            description += row_dict['Job Code/Title']
    if 'Job Class' in row_dict:
        if not row_dict['Job Class'] is None:
            description += row_dict['Job Class']
    return description


def prepare_json_for_xmldatastore(dict_object, DataProperties):
    (dict_object['job_info'])['nounPhrases'] = DataProperties['nounPhrases']
    (dict_object['job_info'])['documentType'] = DataProperties['documentType']
    (dict_object['job_info'])['dataSource'] = DataProperties['dataSource']
    (dict_object['job_info'])['description'] = (
        dict_object['job_info'])['job_description']
    del (dict_object['job_info'])['job_description']
    (dict_object['job_info'])['doc_id'] = DataProperties['doc_id']
    if not (dict_object['job_info'])['job_title'] is None:
        (dict_object['job_info'])['documentTitle'] = (
            dict_object['job_info'])['job_title']
    else:
        (dict_object['job_info'])['documentTitle'] = 'Undefined'
    (dict_object['job_info'])['documentDesc'] = (
        (dict_object['job_info'])['description'])[0:200]
    jsonstring = utility.dict_to_json(dict_object)
    return jsonstring


def prepare_dict_for_exceldatastore(dict_object, DataProperties):
    (dict_object)['nounPhrases'] = DataProperties['nounPhrases']
    (dict_object)['documentType'] = DataProperties['documentType']
    (dict_object)['dataSource'] = DataProperties['dataSource']
    (dict_object)['description'] = DataProperties['description']
    (dict_object)['doc_id'] = DataProperties['doc_id']
    if 'Job Title' in dict_object:
        if not (dict_object)['Job Title'] is None:
            (dict_object)['documentTitle'] = (dict_object)['Job Title']
        else:
            (dict_object)['documentTitle'] = 'Undefined'
    if 'Job Code/Title' in dict_object:
        if not (dict_object)['Job Code/Title'] is None:
            (dict_object)['documentTitle'] = (dict_object)['Job Code/Title']
        else:
            (dict_object)['documentTitle'] = 'Undefined'
    (dict_object)['documentDesc'] = ((dict_object)['description'])[0:200]
    return dict_object


def data_from_DB(connStr, queryId, documentType, dataSource, docid_count):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    query = query_variable_replace(query, documentType, dataSource)
    print(query)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict = {}
    data_dict_list = []
    for row in db_data:
        try:
            docid_count += 1
            data_dict = dict(utility.zip_list(column_headers, row))
            data_dict['description'] = ''
            data_dict['documentTitle'] = ''
            data_dict['documentDesc'] = ''
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().JobDetails:
                    print(dataSource, documentType, docid_count)
                    data_dict = prepare_dictionary_for_STJobDetails(data_dict)
                if documentType == config.ConfigManager().CandidateDetails:
                    print(dataSource, documentType, docid_count)
                    data_dict = prepare_dictionary_for_STCandidateDetails(
                        data_dict)
            if dataSource == config.ConfigManager().Xchange:
                if documentType == config.ConfigManager().JobDetails:
                    print(dataSource, documentType, docid_count)
                    data_dict = prepare_dictionary_for_XchangeJobDetails(
                        data_dict)
                if documentType == config.ConfigManager().CandidateDetails:
                    print(dataSource, documentType, docid_count)
                    data_dict = prepare_dictionary_for_XchangeCandidateDetails(
                        data_dict)
            noun_phrases = ''
            keywordPlusSkillsnoun_Phrases = ''
            data_dict['nounPhrases'] = noun_phrases
            data_dict['nounPhrasesKeywordPlusSkills'] = keywordPlusSkillsnoun_Phrases
            data_dict['documentType'] = documentType
            data_dict['dataSource'] = dataSource
            data_dict['doc_id'] = docid_count
            data_dict['descriptionOld'] = data_dict['description']
            data_dict['matchIndexProcess'] = 0
            data_dict = utility.decimal_to_float(data_dict)
            data_dict_list.append(data_dict)
        except BaseException as ex:
            utility.log_exception(ex)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_STdata_to_DB(data_dict_list, connection, documentType)
        if dataSource == config.ConfigManager().Xchange:
            insert_xCHANGEdata_to_DB(data_dict_list, connection, documentType)
    if not data_dict:
        print('Do nothing')
    else:
        primary_key_store(data_dict, documentType, dataSource, connection)
    return docid_count


def data_changes_from_DB(connStr, queryId, documentType, dataSource, dateModified):

    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    configdocs = retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    if '.' in str(dateModified):
        query = query.replace('##dateModified##', str(dateModified).split('.')[0])
# + ('.') + str(configdocs[0]['submissionsdateCreated']).split('.')[1][:3] 
    else:
        query = query.replace('##dateModified##', str(dateModified))
    print(query)
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
            data_dict['description'] = ''
            data_dict['documentTitle'] = ''
            data_dict['documentDesc'] = ''
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().JobDetails:
                    print(dataSource, documentType)
                    data_dict = prepare_dictionary_for_STJobDetailChanges(data_dict)
                if documentType == config.ConfigManager().CandidateDetails:
                    print(dataSource, documentType)
                    data_dict = prepare_dictionary_for_STCandidateDetailChanges(data_dict)
            if dataSource == config.ConfigManager().Xchange:
            #     if documentType == config.ConfigManager().JobDetails:
            #         print(dataSource, documentType, docid_count)
            #         data_dict = prepare_dictionary_for_XchangeJobDetails(
            #             data_dict)
                if documentType == config.ConfigManager().CandidateDetails:
                    print(dataSource, documentType)
                    data_dict = prepare_dictionary_for_XchangeCandidateDetails(
                         data_dict)
                    keywordPlusSkillsnoun_Phrases = dcrnlp.extract_nounphrases_sentences(data_dict['keywordPlusSkills'])

            noun_phrases = dcrnlp.extract_nounphrases_sentences(data_dict['description'])

            # keywordPlusSkillsnoun_Phrases = ''
            data_dict['nounPhrases'] = noun_phrases
            # data_dict['nounPhrasesKeywordPlusSkills'] = keywordPlusSkillsnoun_Phrases
            
            if dataSource == config.ConfigManager().ST:
                # print(data_dict)
                update_STdata_to_DB(data_dict, connection, documentType)
                # print(data_dict)
            if dataSource == config.ConfigManager().Xchange:
                update_xCHANGEdata_to_DB(data_dict, connection, documentType)
                # print(data_dict)

        except BaseException as ex:
            utility.log_exception(ex)

    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().JobDetails:
            if 'dateModified' in data_dict:
                UpdateTemplateSet['STRequirementChangesDate'] = data_dict['dateModified']
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)
        if documentType == config.ConfigManager().CandidateDetails:
            if 'dateModified' in data_dict:
                UpdateTemplateSet['STCandidateChangesDate'] = data_dict['dateModified']
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)
    if dataSource == config.ConfigManager().Xchange:
            #     if documentType == config.ConfigManager().JobDetails:
            #         print(dataSource, documentType, docid_count)
            #         data_dict = prepare_dictionary_for_XchangeJobDetails(
            #             data_dict)
        if documentType == config.ConfigManager().CandidateDetails:
            if 'modifieddate' in data_dict:
                UpdateTemplateSet['xCHANGECandidateChangesDate'] = data_dict['modifieddate']
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


def insert_rates_data_to_DB(data_dict, connection):
    try:
        data_base = dbmanager.db_connection(
            connection, config.ConfigManager().RatesDB)
        collection = dbmanager.retrieve_collection(
            data_base, config.ConfigManager().stagingCollection)
        dbmanager.crud_create(
            data_base, config.ConfigManager().stagingCollection, data_dict)
    except BaseException as ex:
        exception_message = '\n' + 'Exception:' + \
            str(datetime.datetime.now()) + '\n'
        exception_message += '\n' + str(ex) + '\n'
        exception_message += '-' * 100
        utility.write_to_file(
            config.ConfigManager().LogFile, 'a', exception_message)


def rates_prepare_dictionary(data_dict, documentType, document):
    description = ''
    if document:
        if 'resumeText' in document[0]:
            if not document[0]['resumeText'] is None:
                if document[0]['resumeText'] != '':
                    data_dict['resumeText'] = document[0]['resumeText']
        if 'reqFileDesc' in document[0]:
            if not document[0]['reqFileDesc'] is None:
                if document[0]['reqFileDesc'] != '':
                    data_dict['reqFileDesc'] = document[0]['reqFileDesc']
    if 'jobTitle' in data_dict:
        if not data_dict['jobTitle'] is None:
            if data_dict['jobTitle'] != '':
                description += data_dict['jobTitle'] + ' '
    if 'jobDescription' in data_dict:
        if not data_dict['jobDescription'] is None:
            if data_dict['jobDescription'] != '':
                description += data_dict['jobDescription'] + ' '
    if 'mandatorySkills' in data_dict:
        if not data_dict['mandatorySkills'] is None:
            if data_dict['mandatorySkills'] != '':
                description += data_dict['mandatorySkills'] + ' '
    if 'desiredSkills' in data_dict:
        if not data_dict['desiredSkills'] is None:
            if data_dict['desiredSkills'] != '':
                description += data_dict['desiredSkills'] + ' '
    if 'SkillsRequired' in data_dict:
        if not data_dict['SkillsRequired'] is None:
            if data_dict['SkillsRequired'] != '':
                description += data_dict['SkillsRequired'] + ' '
    if documentType == config.ConfigManager().STReqCandidateRatesDetails:
        if 'resumeText' in data_dict:
            if not data_dict['resumeText'] is None:
                if data_dict['resumeText'] != '':
                    description += data_dict['resumeText'] + ' '
    if documentType == config.ConfigManager().STReqRatesDetails:
        if 'reqFileDesc' in data_dict:
            if not data_dict['reqFileDesc'] is None:
                if data_dict['reqFileDesc'] != '':
                    description += data_dict['reqFileDesc'] + ' '

    data_dict['description'] = description
    data_dict['source'] = "Smart Track"
    data_dict['dateCreated'] = datetime.datetime.utcnow()
    data_dict['dateModified'] = datetime.datetime.utcnow()
    return data_dict


def rates_data_from_DB(connStr, queryId, documentType, dataSource, dateModified):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    configdocs = retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STReqCandidateRatesDetails:
            # if '.' in str(dateModified):
            #     query = query.replace('##candidateSubmissionsDateCreated##', str(dateModified).split('.')[0])
            # # + ('.') + str(configdocs[0]['submissionsdateCreated']).split('.')[1][:3]
            # else:
            #     query = query.replace('##candidateSubmissionsDateCreated##', str(dateModified))
            query = query.replace('##candidateSubmissionsDateCreated##', (str(dateModified))[:-3])
        if documentType == config.ConfigManager().STReqRatesDetails:
            query = query.replace('##reqDateCreated##', (str(dateModified))[:-3])
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    count = 0
    data_dict = {}
    data_dict_list = []
    candidateDatesList = []
    requirementDatesList = []
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STReqCandidateRatesDetails:
                    data_dict['resumeFlag'] = 1
                    condition = {"candidateid": int(data_dict['candidateid'])}
                    connection = dbmanager.mongoDB_connection(int(config.ConfigManager().MongoDBPort))
                    data_base = dbmanager.db_connection(connection, config.ConfigManager().DataCollectionDB)
                    collection = dbmanager.retrieve_collection(data_base, config.ConfigManager().STCandidateCollection)
                    document = dbmanager.crud_read_one(data_base, collection, condition)
                    data_dict = rates_prepare_dictionary(data_dict, documentType, document)
                    data_dict_list.append(data_dict)
                    if 'stcanddatecreated' in data_dict:
                        candidateDatesList.append(data_dict['stcanddatecreated'])
                if documentType == config.ConfigManager().STReqRatesDetails:
                    condition = {"requirementid": int(data_dict['requirementid'])}
                    connection = dbmanager.mongoDB_connection(int(config.ConfigManager().MongoDBPort))
                    data_base = dbmanager.db_connection(connection, config.ConfigManager().DataCollectionDB)
                    collection = dbmanager.retrieve_collection(data_base, config.ConfigManager().STReqCollection)
                    document = dbmanager.crud_read_one(data_base, collection, condition)
                    data_dict = rates_prepare_dictionary(data_dict, documentType, document)
                    data_dict_list.append(data_dict)
                    if 'streqdatecreated' in data_dict:
                        requirementDatesList.append(data_dict['streqdatecreated'])
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_rates_data_to_DB(data_dict_list, connection)

    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STReqCandidateRatesDetails:
            if 'stcanddatecreated' in data_dict:
                maxCandDate = max(candidateDatesList)
                UpdateTemplateSet['STReqCandidateRatesDate'] = str(maxCandDate)
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)
        if documentType == config.ConfigManager().STReqRatesDetails:
            if 'streqdatecreated' in data_dict:
                maxReqDate = max(requirementDatesList)
                UpdateTemplateSet['STReqRatesDate'] = str(maxReqDate)
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


def prepare_dictionary_for_STJobDetails(data_dict):
    if not data_dict['jobDescription'] is None:
        data_dict['description'] += data_dict['jobDescription']
    if not data_dict['SkillDescMandatory'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescMandatory']
    if not data_dict['SkillDescDesired'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescDesired']
    if not data_dict['TypeofService'] is None:
        data_dict['description'] += '. ' + \
            data_dict['TypeofService']
    if not data_dict['jobTitleName'] is None:
        data_dict['documentTitle'] = data_dict['jobTitleName']
    else:
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['description'] == '':
        data_dict['documentDesc'] = (
            data_dict['description'])[0:200]
    return data_dict


def prepare_dictionary_for_STReqCandidateRatesDetails(data_dict):
    if not data_dict['jobDescription'] is None:
        data_dict['description'] += data_dict['jobDescription']
    if not data_dict['SkillDescMandatory'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescMandatory']
    if not data_dict['SkillDescDesired'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescDesired']
    if not data_dict['TypeofService'] is None:
        data_dict['description'] += '. ' + \
            data_dict['TypeofService']
    if not data_dict['jobTitleName'] is None:
        data_dict['documentTitle'] = data_dict['jobTitleName']
    else:
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['description'] == '':
        data_dict['documentDesc'] = (
            data_dict['description'])[0:200]
    return data_dict


def prepare_dictionary_for_STReqRatesDetails(data_dict):
    if not data_dict['jobDescription'] is None:
        data_dict['description'] += data_dict['jobDescription']
    if not data_dict['SkillDescMandatory'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescMandatory']
    if not data_dict['SkillDescDesired'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescDesired']
    if not data_dict['TypeofService'] is None:
        data_dict['description'] += '. ' + \
            data_dict['TypeofService']
    if not data_dict['jobTitleName'] is None:
        data_dict['documentTitle'] = data_dict['jobTitleName']
    else:
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['description'] == '':
        data_dict['documentDesc'] = (
            data_dict['description'])[0:200]
    return data_dict


def prepare_dictionary_for_STCandidateDetails(data_dict):
    if not data_dict['skillname'] is None:
        data_dict['description'] += data_dict['skillname']
    if not data_dict['skilldescription'] is None:
        data_dict['description'] += '. ' + \
            data_dict['skilldescription']
    if not data_dict['firstName'] is None:
        data_dict[
            'documentTitle'] += data_dict['firstName'] + ' '
    if not data_dict['lastName'] is None:
        data_dict[
            'documentTitle'] += data_dict['lastName'] + ' '
    if not data_dict['skillname'] is None:
        data_dict['documentTitle'] += data_dict['skillname']
    if data_dict['documentTitle'] == '':
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['skilldescription'] is None:
        data_dict['documentDesc'] = (
            data_dict['skilldescription'])[0:200]
    return data_dict


def prepare_dictionary_for_XchangeJobDetails(data_dict):
    # if not data_dict['Description'] is None:
        # data_dict['description'] += data_dict['Description']
    # if not data_dict['Title'] is None:
        # data_dict['description'] += '. ' + data_dict['Title']
    # if not data_dict['MandatorySkill'] is None:
        # data_dict['description'] += '. ' + \
            # data_dict['MandatorySkill']
    # if not data_dict['DesiredSkill'] is None:
        # data_dict['description'] += '. ' + \
            # data_dict['DesiredSkill']
    # del data_dict['Description']
    # if not data_dict['Title'] is None:
        # data_dict['documentTitle'] = data_dict['Title']
    # else:
        # data_dict['documentTitle'] = 'Undefined'
    # if not data_dict['description'] == '':
        # data_dict['documentDesc'] = (
            # data_dict['description'])[0:200]
    return data_dict


def prepare_dictionary_for_XchangeCandidateDetails(data_dict):
    data_dict['keywordPlusSkills'] = ''
    if not data_dict['headline'] is None:
        data_dict['description'] += data_dict['headline']
    if not data_dict['experience'] is None:
        data_dict['description'] += '. ' + \
            data_dict['experience']
    if not data_dict['skill'] is None:
        data_dict['description'] += '. ' + data_dict['skill']
    if not data_dict['keyword'] is None:
        data_dict['description'] += '. ' + data_dict['keyword']
    if not data_dict['skill'] is None:
        data_dict['keywordPlusSkills'] += data_dict['skill']
    if not data_dict['keyword'] is None:
        if data_dict['keywordPlusSkills'] == '':
            data_dict['keywordPlusSkills'] += data_dict['keyword']
        else:
            data_dict['keywordPlusSkills'] += '. ' + data_dict['keyword']
    if not data_dict['firstname'] is None:
        data_dict[
            'documentTitle'] += data_dict['firstname'] + ' '
    if not data_dict['lastname'] is None:
        data_dict[
            'documentTitle'] += data_dict['lastname'] + ' '
    if not data_dict['headline'] is None:
        data_dict['documentTitle'] += data_dict['headline']
    if data_dict['documentTitle'] == '':
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['experience'] is None:
        data_dict['documentDesc'] = (
            data_dict['experience'])[0:200]
    data_dict['description'].replace("SkillName :", "").replace("Keyword :", "")
    data_dict['keywordPlusSkills'].replace("SkillName :", "").replace("Keyword :", "")
    return data_dict

def prepare_dictionary_for_STJobDetailChanges(data_dict):
    if not data_dict['jobDescription'] is None:
        data_dict['description'] += data_dict['jobDescription']
    if not data_dict['SkillDescMandatory'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescMandatory']
    if not data_dict['SkillDescDesired'] is None:
        data_dict['description'] += '. ' + \
            data_dict['SkillDescDesired']
    if not data_dict['TypeofService'] is None:
        data_dict['description'] += '. ' + \
            data_dict['TypeofService']
    if not data_dict['jobTitleName'] is None:
        data_dict['documentTitle'] = data_dict['jobTitleName']
    else:
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['description'] == '':
        data_dict['documentDesc'] = (
            data_dict['description'])[0:200]
    data_dict['descriptionOld'] = data_dict['description']
    if 'resumeText' in data_dict:
        data_dict['description'] += data_dict['resumeText']
    data_dict['matchIndexProcess'] = 0
    return data_dict

def prepare_dictionary_for_STCandidateDetailChanges(data_dict):
    if not data_dict['skillname'] is None:
        data_dict['description'] += data_dict['skillname']
    if not data_dict['skilldescription'] is None:
        data_dict['description'] += '. ' + \
            data_dict['skilldescription']
    if not data_dict['firstName'] is None:
        data_dict[
            'documentTitle'] += data_dict['firstName'] + ' '
    if not data_dict['lastName'] is None:
        data_dict[
            'documentTitle'] += data_dict['lastName'] + ' '
    if not data_dict['skillname'] is None:
        data_dict['documentTitle'] += data_dict['skillname']
    if data_dict['documentTitle'] == '':
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['skilldescription'] is None:
        data_dict['documentDesc'] = (
            data_dict['skilldescription'])[0:200]
    data_dict['descriptionOld'] = data_dict['description']
    data_dict['matchIndexProcess'] = 0
    return data_dict

def prepare_dictionary_for_XchangeCandidateDetailChanges(data_dict):
    data_dict['keywordPlusSkills'] = ''
    if not data_dict['headline'] is None:
        data_dict['description'] += data_dict['headline']
    if not data_dict['experience'] is None:
        data_dict['description'] += '. ' + \
            data_dict['experience']
    if not data_dict['skill'] is None:
        data_dict['description'] += '. ' + data_dict['skill']
    if not data_dict['keyword'] is None:
        data_dict['description'] += '. ' + data_dict['keyword']
    if not data_dict['skill'] is None:
        data_dict['keywordPlusSkills'] += data_dict['skill']
    if not data_dict['keyword'] is None:
        if data_dict['keywordPlusSkills'] == '':
            data_dict['keywordPlusSkills'] += data_dict['keyword']
        else:
            data_dict['keywordPlusSkills'] += '. ' + data_dict['keyword']
    if not data_dict['firstname'] is None:
        data_dict[
            'documentTitle'] += data_dict['firstname'] + ' '
    if not data_dict['lastname'] is None:
        data_dict[
            'documentTitle'] += data_dict['lastname'] + ' '
    if not data_dict['headline'] is None:
        data_dict['documentTitle'] += data_dict['headline']
    if data_dict['documentTitle'] == '':
        data_dict['documentTitle'] = 'Undefined'
    if not data_dict['experience'] is None:
        data_dict['documentDesc'] = (
            data_dict['experience'])[0:200]
    data_dict['description'].replace("SkillName :" , "").replace("Keyword :" , "")
    data_dict['keywordPlusSkills'].replace("SkillName :" , "").replace("Keyword :" , "")
    data_dict['descriptionOld'] = data_dict['description']
    data_dict['matchIndexProcess'] = 0
    return data_dict


def fetch_query(queryId):
    dictionaries.DBWhereConditon = {}
    (dictionaries.DBWhereConditon)['query_id'] = int(queryId)
    condition = utility.jsonstring_deserialize(
        utility.dict_to_json(dictionaries.DBWhereConditon))
    query_document = retrieve_rowdata_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().QueryCollection, condition)
    query = (query_document[0])['query']
    return query


def primary_key_store(data_dict, documentType, dataSource, connection):
    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    DBSet = utility.clean_dict()
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().JobDetails:
            primary_key = data_dict['requirementid']
            dictionaries.UpdateTemplateSet['STjobpk'] = primary_key
        if documentType == config.ConfigManager().STSupplierDetails:
            print(data_dict['supplierID'])
            primary_key = data_dict['supplierID']
            dictionaries.UpdateTemplateSet['STSupplierNamespk'] = primary_key
        if documentType == config.ConfigManager().CandidateDetails:
            primary_key = data_dict['candidateid']
            dictionaries.UpdateTemplateSet['STcandidatepk'] = primary_key
    if dataSource == config.ConfigManager().Xchange:
        if documentType == config.ConfigManager().JobDetails:
            primary_key = data_dict['JobID']
            dictionaries.UpdateTemplateSet['Xchangejobpk'] = primary_key
        if documentType == config.ConfigManager().CandidateDetails:
            primary_key = data_dict['UserProfileID']
            dictionaries.UpdateTemplateSet['Xchangecandidatepk'] = primary_key
    configdocs = retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager().ConfigCollection)
    dictionaries.UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
    print(primary_key)
    update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet, connection)


def query_variable_replace(query, documentType, dataSource):
    configdocs = retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager().ConfigCollection)
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().JobDetails:
            query = query.replace('##requirementid##',
                                  str(configdocs[0]['STjobpk']))
        if documentType == config.ConfigManager().CandidateDetails:
            query = query.replace('##candidateid##', str(
                configdocs[0]['STcandidatepk']))
        if documentType == config.ConfigManager().STSupplierDetails:
            query = query.replace('##supplierID##', str(
                configdocs[0]['STSupplierNamespk']))
        if documentType == config.ConfigManager().STCandidateCurrencyDetails:
            query = query.replace('##dateCreated##', str(
                configdocs[0]['STCandidateCurrencyCodeLastDate'])[:-3])
    if dataSource == config.ConfigManager().Xchange:
        if documentType == config.ConfigManager().JobDetails:
            query = query.replace('##JobID##', str(
                configdocs[0]['Xchangejobpk']))
        if documentType == config.ConfigManager().CandidateDetails:
            query = query.replace('##UserProfileID##', str(
                configdocs[0]['Xchangecandidatepk']))
    return query


def query_variable_replace_for_rates(query, documentType, dataSource):
    configdocs = retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().RatesDB, config.ConfigManager().RatesConfigCollection)
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STClientDetails:
            query = query.replace('##clientID##',
                                  str(configdocs[0]['clientID']))
        if documentType == config.ConfigManager().STMSPDetails:
            query = query.replace('##mspID##', str(
                configdocs[0]['mspID']))
        if documentType == config.ConfigManager().STlaborCatagoryDetails:
            query = query.replace('##laborCatagory##', str(
                configdocs[0]['laborCatagory']))
        if documentType == config.ConfigManager().STTypeofServiceDetails:
            query = query.replace('##typeOfService##', str(
                configdocs[0]['typeOfService']))
        if documentType == config.ConfigManager().currencyDetails:
            query = query.replace('##currencyID##', str(
                configdocs[0]['currencyID']))
        if documentType == config.ConfigManager().industryDetails:
            query = query.replace('##industryID##', str(
                configdocs[0]['industryID']))
        if documentType == config.ConfigManager().STSupplierDetails:
            query = query.replace('##supplierID##', str(
                configdocs[0]['STSupplierNamespk']))
    return query


def data_from_sqlDB_forSuppliers(connStr, queryId, documentType, dataSource, supplierID):

    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    query = query_variable_replace(query, documentType, dataSource)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']

    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict = {}
    data_dict_list = []
    supplierId_list = []
    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STSupplierDetails:
                    supplierID = data_dict['supplierID']
                    supplierId_list.append(supplierID)
                    print(dataSource, documentType, supplierID)
            data_dict_list.append(data_dict)
        except BaseException as ex:
            utility.log_exception(ex)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_STdata_to_DB(data_dict_list, connection, documentType)
            insert_STdata_to_RatesDB(data_dict_list, connection, documentType)
        return max(supplierId_list)
    else:
        return 0


def data_from_sqlDB_toMongo(connStr, queryId, documentType, dataSource, supplierID):

    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    query = query_variable_replace_for_rates(query, documentType, dataSource)
    print(query)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']

    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict = {}
    data_dict_list = []
    supplierId_list = []
    for row in db_data:
        # print(row)
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STClientDetails:
                    supplierID = data_dict['clientID']
                    supplierId_list.append(supplierID)
                if documentType == config.ConfigManager().STMSPDetails:
                    supplierID = data_dict['mspID']
                    supplierId_list.append(supplierID)
                if documentType == config.ConfigManager().STlaborCatagoryDetails:
                    supplierID = data_dict['laborClassID']
                    supplierId_list.append(supplierID)
                if documentType == config.ConfigManager().STTypeofServiceDetails:
                    supplierID = data_dict['typeofServiceID']
                    supplierId_list.append(supplierID)
                if documentType == config.ConfigManager().currencyDetails:
                    supplierID = data_dict['currencyID']
                    supplierId_list.append(supplierID)
                if documentType == config.ConfigManager().industryDetails:
                    supplierID = data_dict['industryID']
                    supplierId_list.append(supplierID)
            data_dict_list.append(data_dict)
        except BaseException as ex:
            utility.log_exception(ex)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_STdata_to_RatesDB(data_dict_list, connection, documentType)
        return max(supplierId_list)
    else:
        return 0


def getpcdata(data_dict):
    cl = MongoClient(config.ConfigManager().DataDbQA)
    db = cl[config.ConfigManager().PCDataLoaddb]
    datacol = db[config.ConfigManager().collStaging]
    try:
        # recdata = datacol.find_one({"_id": data_dict['GUID1']}, {"_id": 0}) //Original Query
        recdata = datacol.find_one({"uniq_id": data_dict['GUID2']}, {"_id": 0, "jobdescription":1, "JobTitle":1})
    except BaseException as ex:
        utility.log_exception(ex)
    # for data in recdata:
    #     data_dict[data] = recdata[data]
    if 'jobdescription' in recdata and recdata['jobdescription'] != '':
        data_dict['jobDescription'] = recdata['jobdescription']
        data_dict['description'] = ''
        data_dict['description'] += data_dict['jobDescription']
        if 'JobTitle' in recdata and recdata['JobTitle'] != '':
            data_dict['jobTitle'] = recdata['JobTitle']
            data_dict['description'] += data_dict['jobTitle']
    return data_dict


def normalize_pc_data(data_dict):
    # print(" --> Control Reached Here")
    data_dict['typeOfService'] = data_dict['TypeOfService']
    data_dict['country'] = data_dict['Country']
    data_dict['city'] = data_dict['City']
    data_dict['state'] = data_dict['State']
    if 'Unit' in data_dict and data_dict['Unit'] != '':
        data_dict['ratetime_interval'] = data_dict['Unit']
    data_dict['minBillRate'] = data_dict['MinRate']
    data_dict['maxBillRate'] = data_dict['MaxRate']
    data_dict['zipCode'] = data_dict['PostalCode']
    data_dict['source'] = config.ConfigManager().promptCloud
    if 'Url' in data_dict and data_dict['Url'] != '':
        data_dict['dataSource'] = data_dict['Url']
    else:
        data_dict['dataSource'] = ""
    data_dict['dateCreated'] = datetime.datetime.utcnow()
    data_dict['dateModified'] = datetime.datetime.utcnow()
    return data_dict


def rates_data_from_Promptcloud_DB(connStr, queryId, documentType, dataSource):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)
    configdocs = retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort),
                                       config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection)

    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    count = 0
    data_dict = {}
    data_dict_list = []
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            data_dict = getpcdata(data_dict)
            # print("Control is in the between of PC_Data and Normalized Data")
            data_dict = normalize_pc_data_new(data_dict)
            if dataSource == config.ConfigManager().ST:
                data_dict_list.append(data_dict)
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)

    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_rates_data_to_DB(data_dict_list, connection)


def getgeolocations(connStr, queryId, documentType, dataSource):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query(queryId)

    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    count = 0
    data_dict = {}
    data_dict_list = []

    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            data_dict_list.append(data_dict)
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)
    return data_dict_list


def create_sql_dict_list(query, connStr):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    dictList = []
    dictList = [(dict(utility.zip_list(column_headers, row))) for row in db_data]
    return dictList


def geo_data_check(row_dict, dictList, geoCode):
    geoDataMatch = False
    valueAccuracyCheckList = []
    if geoCode in row_dict:
        if row_dict[geoCode] != '' and row_dict[geoCode] is not None:
            if geoCode == 'country':
                checkValue = row_dict['country']
                checkValue = checkValue.strip()
                valueAccuracyCheckList = [x for x in dictList if ((checkValue).lower() == (x['name']).lower() or (checkValue).lower() == (x['iso_alpha3']).lower() or (checkValue).lower() == (x['fips_code']).lower())]
                if valueAccuracyCheckList:
                    row_dict['country'] = (valueAccuracyCheckList[0])['name']
                    row_dict['iso_alpha2_value'] = (valueAccuracyCheckList[0])['iso_alpha2']
                    row_dict['countryLocationFlag'] = 1
                else:
                    row_dict['countryLocationFlag'] = 0
            if geoCode == 'state':
                checkValue = row_dict['state']
                checkValue = checkValue.strip()
                valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['name']).lower() and (row_dict['iso_alpha2_value']).lower() in (x['code']).lower()) or ((checkValue).lower() == (x['admin1']).lower() and (row_dict['iso_alpha2_value']).lower() in (x['code']).lower()))]
                if valueAccuracyCheckList:
                    valueAccuracyCheckListItem = valueAccuracyCheckList[0]
                    row_dict['state'] = valueAccuracyCheckListItem['name']
                    row_dict['stateLatitude'] = str(valueAccuracyCheckListItem['latitude'])
                    row_dict['stateLongitude'] = str(valueAccuracyCheckListItem['longitude'])
                    # stateLatitude = valueAccuracyCheckListItem['latitude']
                    # stateLongitude = valueAccuracyCheckListItem['longitude']
                    # row_dict['stateLatitude'] = str(stateLatitude)
                    # row_dict['stateLongitude'] = str(stateLongitude)
                    # row_dict['stateGeoLocation'] = [stateLongitude, stateLatitude]
                    row_dict['stateLocationFlag'] = 1
                    row_dict['admin1_value'] = valueAccuracyCheckListItem['admin1']
                    row_dict['state_name'] = valueAccuracyCheckListItem['name']
                else:
                    row_dict['stateLocationFlag'] = 0
            if geoCode == 'city':
                checkValue = row_dict['city']
                checkValue = checkValue.strip()
                checkValueList = checkValue.split('#')
                if len(checkValueList) > 0:
                    checkValue = (checkValueList[0]).strip()
                if row_dict['iso_alpha2_value'] != ')(*&^':
                    if (row_dict['state_name'] != ')(*&^' or row_dict['admin1_value'] != ')(*&^'):
                        valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                    else:
                        valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['iso_alpha2_value']).lower() == (x['sCountryCode']).lower()) or ((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                else:
                    valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['iso_alpha2_value']).lower() == (x['sCountryCode']).lower()) or ((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPlaceName']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                if valueAccuracyCheckList:
                    valueAccuracyCheckListItem = valueAccuracyCheckList[0]
                    # row_dict['cityLatitude'] = str(valueAccuracyCheckListItem['fLatitude'])
                    # row_dict['cityLongitude'] = str(valueAccuracyCheckListItem['fLongitude'])
                    # row_dict['coordinates'] = [row_dict['cityLatitude'], row_dict['cityLongitude']]
                    cityLatitude = valueAccuracyCheckListItem['fLatitude']
                    cityLongitude = valueAccuracyCheckListItem['fLongitude']
                    row_dict['cityLatitude'] = str(cityLatitude)
                    row_dict['cityLongitude'] = str(cityLongitude)
                    row_dict['coordinates'] = [cityLongitude, cityLatitude]
                    row_dict['cityLocationFlag'] = 1
                else:
                    row_dict['cityLocationFlag'] = 0
            if geoCode == 'zipCode':
                checkValue = str(row_dict['zipCode'])
                checkValue = checkValue.strip()
                if checkValue[-2:] == '.0':
                    checkValue = checkValue[:-2]
                if row_dict['iso_alpha2_value'] != ')(*&^':
                    if (row_dict['state_name'] != ')(*&^' or row_dict['admin1_value'] != ')(*&^'):
                        valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                    else:
                        valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['iso_alpha2_value']).lower() == (x['sCountryCode']).lower()) or ((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                else:
                    valueAccuracyCheckList = [x for x in dictList if (((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['iso_alpha2_value']).lower() == (x['sCountryCode']).lower()) or ((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['state_name']).lower() == (x['sAdminName1']).lower()) or ((checkValue).lower() == (x['sPostalCode']).lower() and (row_dict['admin1_value']).lower() == (x['sAdminCode1']).lower()))]
                if valueAccuracyCheckList:
                    valueAccuracyCheckListItem = valueAccuracyCheckList[0]
                    row_dict['zipCodeLatitude'] = str(valueAccuracyCheckListItem['fLatitude'])
                    row_dict['zipCodeLongitude'] = str(valueAccuracyCheckListItem['fLongitude'])
                    row_dict['zipCodeLocationFlag'] = 1
                else:
                    row_dict['zipCodeLocationFlag'] = 0
        else:
            if geoCode == 'country':
                row_dict['countryLocationFlag'] = 0
            if geoCode == 'state':
                row_dict['stateLocationFlag'] = 0
            if geoCode == 'city':
                row_dict['cityLocationFlag'] = 0
            if geoCode == 'zipCode':
                row_dict['zipCodeLocationFlag'] = 0
    else:
        if geoCode == 'country':
            row_dict['countryLocationFlag'] = 0
        if geoCode == 'state':
            row_dict['stateLocationFlag'] = 0
        if geoCode == 'city':
            row_dict['cityLocationFlag'] = 0
        if geoCode == 'zipCode':
            row_dict['zipCodeLocationFlag'] = 0
    return row_dict


def retrieve_data_from_db_with_host_input(Port, DB, Col, host):
    try:
        connection = dbmanager.mongo_db_connection_with_host(int(Port), host)
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        documents = dbmanager.crud_read(data_base, collection)
        return documents
    except BaseException as ex:
        utility.log_exception_file(config.ConfigManager().LogFile, ex)


def supplier_master_data_read(connStr, queryId, documentType, dataSource, supplierID):

    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query_with_host(queryId, config.ConfigManager().ExternalHost)
    # print("Query =>", query,"Supplier ID in Function => ",supplierID)
    query = query_variable_replace_for_rates(query, documentType, dataSource)
    # print("Query after variable replace =>", query)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    # print("DB data dict", db_data_dict)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']

    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict = {}
    data_dict_list = []
    supplierId_list = []
    for row in db_data:
        # print("row =>",row)
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            # print(data_dict)
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STSupplierDetails:
                    supplierID = data_dict['supplierID']
                    supplierId_list.append(supplierID)
                    print(dataSource, documentType, supplierID)
            data_dict_list.append(data_dict)
        except BaseException as ex:
            utility.log_exception_file(config.ConfigManager().LogFile, ex)
    print("data dictionary list => ",data_dict_list)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_STdata_to_RatesDB(data_dict_list, connection, documentType)
        return max(supplierId_list)
    else:
        return 0


def fetch_query_with_host(queryId, host):
    dictionaries.DBWhereConditon = {}
    (dictionaries.DBWhereConditon)['query_id'] = int(queryId)
    condition = utility.jsonstring_deserialize(
        utility.dict_to_json(dictionaries.DBWhereConditon))
    query_document = retrieve_row_data_from_db_with_host_input(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().QueryCollection, condition, host)
    query = (query_document[0])['query']
    return query


def retrieve_row_data_from_db_with_host_input(Port, DB, Col, condition, host):
    try:
        connection = dbmanager.mongo_db_connection_with_host(int(Port), host)
        data_base = dbmanager.db_connection(connection, DB)
        collection = dbmanager.retrieve_collection(data_base, Col)
        document = dbmanager.crud_read_one(data_base, collection, condition)
        return document
    except BaseException as ex:
        utility.log_exception_file(config.ConfigManager().LogFile, ex)


def master_data_transfer_from_sql(connStr, queryId, documentType, dataSource, Id, host):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query_with_host(queryId, host)
    query = query_variable_replace_for_rates(query, documentType, dataSource)
    print(query)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']

    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    data_dict = {}
    data_dict_list = []
    Id_list = []
    for row in db_data:
        # print(row)
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STClientDetails:
                    Id = data_dict['clientID']
                    Id_list.append(Id)
                if documentType == config.ConfigManager().STMSPDetails:
                    Id = data_dict['mspID']
                    Id_list.append(Id)
                if documentType == config.ConfigManager().STlaborCatagoryDetails:
                    Id = data_dict['laborClassID']
                    Id_list.append(Id)
                if documentType == config.ConfigManager().STTypeofServiceDetails:
                    Id = data_dict['typeofServiceID']
                    Id_list.append(Id)
                if documentType == config.ConfigManager().currencyDetails:
                    Id = data_dict['currencyID']
                    Id_list.append(Id)
                if documentType == config.ConfigManager().industryDetails:
                    Id = data_dict['industryID']
                    Id_list.append(Id)
            data_dict_list.append(data_dict)
        except BaseException as ex:
            utility.log_exception(ex)
    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            insert_STdata_to_RatesDB(data_dict_list, connection, documentType)
        return max(Id_list)
    else:
        return Id


def st_rates_master_data(connStr, queryId, documentType, dataSource, dateModified, host):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    query = fetch_query_with_host(queryId, host)
    configdocs = retrieve_data_from_db_with_host_input(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
    ).DataCollectionDB, config.ConfigManager().ConfigCollection, host)
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STReqCandidateRatesDetails:
            # if '.' in str(dateModified):
            #     query = query.replace('##candidateSubmissionsDateCreated##', str(dateModified).split('.')[0])
            # # + ('.') + str(configdocs[0]['submissionsdateCreated']).split('.')[1][:3]
            # else:
            #     query = query.replace('##candidateSubmissionsDateCreated##', str(dateModified))
            query = query.replace('##candidateSubmissionsDateCreated##', (str(dateModified))[:-3])
        if documentType == config.ConfigManager().STReqRatesDetails:
            query = query.replace('##reqDateCreated##', (str(dateModified))[:-3])
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    count = 0
    data_dict = {}
    data_dict_list = []
    candidateDatesList = []
    requirementDatesList = []
    connection = dbmanager.mongo_db_connection_with_host(int(config.ConfigManager().MongoDBPort), config.ConfigManager().ExternalHost)
    data_base = dbmanager.db_connection(connection, config.ConfigManager().DataCollectionDB)
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STReqCandidateRatesDetails:
            candidateIdList = [int((dict(utility.zip_list(column_headers, row)))['candidateid']) for row in db_data]
            candidateIdList = [int(x) for x in candidateIdList]
            condition = {"candidateid": {"$in": candidateIdList}}
            projection = {"candidateid": 1, "resumeText": 1}
            collection = dbmanager.retrieve_collection(data_base, config.ConfigManager().STCandidateCollection)
            candidateRatesData = list(collection.find(condition, projection))
        if documentType == config.ConfigManager().STReqRatesDetails:
            requirementIdList = [int((dict(utility.zip_list(column_headers, row)))['requirementid']) for row in db_data]
            requirementIdList = [int(x) for x in requirementIdList]
            condition = {"requirementid": {"$in": requirementIdList}}
            projection = {"requirementid": 1, "reqFileDesc": 1}
            collection = dbmanager.retrieve_collection(data_base, config.ConfigManager().STReqCollection)
            requirementRatesData = list(collection.find(condition, projection))

    for row in db_data:
        try:
            data_dict = dict(utility.zip_list(column_headers, row))
            # print(datetime.datetime.now())
            count += 1
            print(count)
            if dataSource == config.ConfigManager().ST:
                if documentType == config.ConfigManager().STReqCandidateRatesDetails:
                    data_dict['resumeFlag'] = 1
                    if candidateRatesData:
                        document = [element for element in candidateRatesData if int(element['candidateid']) == int(data_dict['candidateid'])]
                    else:
                        document = []
                    data_dict = encode_stratesdataread(data_dict)
                    data_dict = rates_prepare_dictionary(data_dict, documentType, document)
                    if sys.getsizeof(data_dict['description']) < 13000000:
                        data_dict_list.append(data_dict)
                    if 'stcanddatecreated' in data_dict:
                        candidateDatesList.append(data_dict['stcanddatecreated'])
                if documentType == config.ConfigManager().STReqRatesDetails:
                    if requirementRatesData:
                        document = [element for element in requirementRatesData if int(element['requirementid']) == int(data_dict['requirementid'])]
                    else:
                        document = []
                    data_dict = encode_stratesdataread(data_dict)
                    data_dict = rates_prepare_dictionary(data_dict, documentType, document)
                    if sys.getsizeof(data_dict['description']) < 13000000:
                        data_dict_list.append(data_dict)
                    if 'streqdatecreated' in data_dict:
                        requirementDatesList.append(data_dict['streqdatecreated'])
        except BaseException as ex:
            utility.log_exception_file(ex, config.ConfigManager().LogFile)

    if data_dict_list:
        if dataSource == config.ConfigManager().ST:
            connectionDb = dbmanager.mongoDB_connection(
              int(config.ConfigManager().MongoDBPort))
            insert_rates_data_to_DB(data_dict_list, connectionDb)

    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    if dataSource == config.ConfigManager().ST:
        if documentType == config.ConfigManager().STReqCandidateRatesDetails:
            if 'stcanddatecreated' in data_dict:
                maxCandDate = max(candidateDatesList)
                UpdateTemplateSet['STReqCandidateRatesDate'] = str(maxCandDate)
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)
        if documentType == config.ConfigManager().STReqRatesDetails:
            if 'streqdatecreated' in data_dict:
                maxReqDate = max(requirementDatesList)
                UpdateTemplateSet['STReqRatesDate'] = str(maxReqDate)
                UpdateTemplateWhere['_id'] = configdocs[0]['_id']
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager(
                ).DataCollectionDB, config.ConfigManager().ConfigCollection, UpdateTemplateWhere, DBSet, connection)


def create_geo_dict(query, connStr, geoSpace):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    geoDict = {}
    if geoSpace == 'Country':
        geoDict1 = {(row[1]).lower(): (dict(utility.zip_list(column_headers, row))) for row in db_data if row[1] != ''}
        geoDict2 = {(row[2]).lower(): (dict(utility.zip_list(column_headers, row))) for row in db_data if row[2] != ''}
        geoDict3 = {(row[3]).lower(): (dict(utility.zip_list(column_headers, row))) for row in db_data if row[3] != ''}
        geoDict = dict(list(geoDict1.items()) + list(geoDict2.items()) + list(geoDict3.items()))
    if geoSpace == 'State':
        geoDict1 = {((row[2]).lower(), (row[0]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[2] != '' and row[0] != '')}
        geoDict2 = {((row[3]).lower(), (row[0]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[0] != '')}
        geoDict = dict(list(geoDict1.items()) + list(geoDict2.items()))
    if geoSpace == 'City':
        geoDict1 = {((row[3]).lower(), (row[2]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[2] != '')}
        geoDict2 = {((row[3]).lower(), (row[0]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[0] != '')}
        geoDict3 = {((row[3]).lower(), (row[1]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[1] != '')}
        geoDict = dict(list(geoDict1.items()) + list(geoDict2.items()) + list(geoDict3.items()))
    if geoSpace == 'zipCode':
        geoDict1 = {((row[3]).lower(), (row[2]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[2] != '')}
        geoDict2 = {((row[3]).lower(), (row[0]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[0] != '')}
        geoDict3 = {((row[3]).lower(), (row[1]).lower()): (dict(utility.zip_list(column_headers, row))) for row in db_data if (row[3] != '' and row[1] != '')}
        geoDict = dict(list(geoDict1.items()) + list(geoDict2.items()) + list(geoDict3.items()))
    return geoDict


def geo_data_verify(row_dict, geoDict, geoCode):
    geoDataMatch = False
    valueAccuracyCheckList = []
    if geoCode in row_dict:
        if row_dict[geoCode] != '' and row_dict[geoCode] is not None:
            if geoCode == 'country':
                checkValue = row_dict['country']
                checkValue = checkValue.strip()

                if ((checkValue).lower()) in geoDict:
                    row_dict['country'] = (geoDict[((checkValue).lower())])['name']
                    row_dict['iso_alpha2_value'] = (geoDict[((checkValue).lower())])['iso_alpha2']
                    row_dict['countryLocationFlag'] = 1
                else:
                    row_dict['countryLocationFlag'] = 0
            if geoCode == 'state':
                checkValue1 = row_dict['state']
                checkValue1 = checkValue1.strip()
                checkValue = (checkValue1.lower(), (row_dict['iso_alpha2_value']).lower())
                if checkValue in geoDict:
                    row_dict['state'] = (geoDict[checkValue])['name']
                    row_dict['stateLatitude'] = str((geoDict[checkValue])['latitude'])
                    row_dict['stateLongitude'] = str((geoDict[checkValue])['longitude'])
                    row_dict['stateLocationFlag'] = 1
                    row_dict['admin1_value'] = (geoDict[checkValue])['admin1']
                    row_dict['state_name'] = (geoDict[checkValue])['name']
                else:
                    row_dict['stateLocationFlag'] = 0
            if geoCode == 'city':
                checkValue = row_dict['city']
                checkValue = checkValue.strip()
                checkValueList = checkValue.split('#')
                if len(checkValueList) > 0:
                    checkValue = (checkValueList[0]).strip()

                checkValue1 = ((checkValue).lower(), (row_dict['state_name']).lower())
                checkValue2 = ((checkValue).lower(), (row_dict['admin1_value']).lower())
                checkValue3 = ((checkValue).lower(), (row_dict['iso_alpha2_value']).lower())
                checkValueFinal = ')(*&^'
                
                # checkValue3 checked first to give least importance to country city combo as opposed to country state combo
                if checkValue3 in geoDict:
                    checkValueFinal = checkValue3
                if checkValue1 in geoDict:
                    checkValueFinal = checkValue1
                if checkValue2 in geoDict:
                    checkValueFinal = checkValue2

                if checkValueFinal != ')(*&^':
                    cityLatitude = (geoDict[checkValueFinal])['fLatitude']
                    cityLongitude = (geoDict[checkValueFinal])['fLongitude']
                    row_dict['cityLatitude'] = str(cityLatitude)
                    row_dict['cityLongitude'] = str(cityLongitude)
                    row_dict['coordinates'] = [cityLongitude, cityLatitude]
                    row_dict['cityLocationFlag'] = 1
                else:
                    row_dict['cityLocationFlag'] = 0
            if geoCode == 'zipCode':
                checkValue = str(row_dict['zipCode'])
                checkValue = checkValue.strip()
                if checkValue[-2:] == '.0':
                    checkValue = checkValue[:-2]

                checkValue1 = ((checkValue).lower(), (row_dict['state_name']).lower())
                checkValue2 = ((checkValue).lower(), (row_dict['admin1_value']).lower())
                checkValue3 = ((checkValue).lower(), (row_dict['iso_alpha2_value']).lower())
                checkValueFinal = ')(*&^'

                if checkValue3 in geoDict:
                    checkValueFinal = checkValue3
                if checkValue1 in geoDict:
                    checkValueFinal = checkValue1
                if checkValue2 in geoDict:
                    checkValueFinal = checkValue2

                if checkValueFinal != ')(*&^':
                    row_dict['zipCodeLatitude'] = str((geoDict[checkValueFinal])['fLatitude'])
                    row_dict['zipCodeLongitude'] = str((geoDict[checkValueFinal])['fLongitude'])
                    row_dict['zipCodeLocationFlag'] = 1
                else:
                    row_dict['zipCodeLocationFlag'] = 0
        else:
            if geoCode == 'country':
                row_dict['countryLocationFlag'] = 0
            if geoCode == 'state':
                row_dict['stateLocationFlag'] = 0
            if geoCode == 'city':
                row_dict['cityLocationFlag'] = 0
            if geoCode == 'zipCode':
                row_dict['zipCodeLocationFlag'] = 0
    else:
        if geoCode == 'country':
            row_dict['countryLocationFlag'] = 0
        if geoCode == 'state':
            row_dict['stateLocationFlag'] = 0
        if geoCode == 'city':
            row_dict['cityLocationFlag'] = 0
        if geoCode == 'zipCode':
            row_dict['zipCodeLocationFlag'] = 0
    return row_dict


def encode_stratesdataread(data_dict):
    if 'jobTitle' in data_dict:
        if not data_dict['jobTitle'] is None:
            data_dict['jobTitle'] = (data_dict['jobTitle']).replace(u"\udbc0", "").replace(u"\udc78", "")
    if 'jobDescription' in data_dict:
        if not data_dict['jobDescription'] is None:
            data_dict['jobDescription'] = (data_dict['jobDescription']).replace(u"\udbc0", "").replace(u"\udc78", "")
    if 'mandatorySkills' in data_dict:
        if not data_dict['mandatorySkills'] is None:
            data_dict['mandatorySkills'] = (data_dict['mandatorySkills']).replace(u"\udbc0", "").replace(u"\udc78", "")
    if 'desiredSkills' in data_dict:
        if not data_dict['desiredSkills'] is None:
            data_dict['desiredSkills'] = (data_dict['desiredSkills']).replace(u"\udbc0", "").replace(u"\udc78", "")
    if 'SkillsRequired' in data_dict:
        if not data_dict['SkillsRequired'] is None:
            data_dict['SkillsRequired'] = (data_dict['SkillsRequired']).replace(u"\udbc0", "").replace(u"\udc78", "")
    if 'SkillsRequired-Candidate' in data_dict:
        if not data_dict['SkillsRequired-Candidate'] is None:
            data_dict['SkillsRequired-Candidate'] = (data_dict['SkillsRequired-Candidate']).replace(u"\udbc0", "").replace(u"\udc78", "")
    return data_dict


def normalize_pc_data_new(data_dict):
    # print(" <----  Control entered into new normalized module  ---->  ")
    if 'typeOfService' in data_dict and data_dict['typeOfService'] != '':
        data_dict['typeOfService'] = data_dict['TypeOfService']
    if 'Country' in data_dict and data_dict['Country'] != '':
        data_dict['country'] = data_dict['Country']
    if 'City' in data_dict and data_dict['City'] != '':
        data_dict['city'] = data_dict['City']
    if 'State' in data_dict and data_dict['State'] != '':
        data_dict['state'] = data_dict['State']
    # if 'MinRate' in data_dict and data_dict['MinRate'] != '' :
    #     data_dict['minBillRate'] = data_dict['MinRate']
    # if 'MaxRate' in data_dict and data_dict['MaxRate'] != '':
    #     data_dict['maxBillRate'] = data_dict['MaxRate']
    
    if 'Unit' in data_dict and data_dict['Unit'] != '':
        data_dict['ratetime_interval'] = data_dict['Unit']
    rate_value = data_dict['MinRate']+"-"+data_dict['MaxRate']
    data_dict['rate_value'] = rate_value

    if 'PostalCode' in data_dict and data_dict['PostalCode'] != '':
        data_dict['zipCode'] = data_dict['PostalCode']
    data_dict['source'] = config.ConfigManager().promptCloud
    if 'Url' in data_dict and data_dict['Url'] != '':
        data_dict['dataSource'] = data_dict['Url']
    else:
        data_dict['dataSource'] = ""
    data_dict['dateCreated'] = datetime.datetime.utcnow()
    data_dict['dateModified'] = datetime.datetime.utcnow()
    return data_dict


