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
import time


def route_dataread(filepaths):
    file_count = 0
    docreadcount = 0
    antiwordemptycount = 0
    counttext = 0
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'req_desc_file_read running' + ' ' + str(datetime.datetime.now()))
    for filepath in filepaths:
        strtimestamp = str(datetime.datetime.now())
        data_text = ''
        try:
            file_count += 1

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
                if data_text == '':
                    antiwordemptycount += 1
                    data_text = datareadfiletypes.read_doc_text_catdoc(
                        filepath, data_text)
                if data_text == '':
                    utility.write_to_file(config.ConfigManager(
                    ).LogFile, 'a', 'Filepath is: ' + filepath)
            elif filepath[-4:].lower() == ".xls":
                data_text = datareadfiletypes.read_excel_text(
                    filepath, data_text)
            elif filepath[-5:].lower() == ".xlsx":
                data_text = datareadfiletypes.read_excel_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".csv":
                data_text = datareadfiletypes.read_csv_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".odt":
                data_text = datareadfiletypes.read_odt_text(
                    filepath, data_text)
            elif filepath[-4:].lower() == ".xml":
                docid_count = custom.process_xml_data(filepath, docid_count)
            strtimestamp += ' ' + str(datetime.datetime.now())
            if not data_text == '':
                counttext += 1
                file_name = utility.filename_from_filepath(filepath)
                requirementId = (file_name[1:]).split('_')[0]
                print(requirementId)
                UpdateTemplateSet = utility.clean_dict()
                UpdateTemplateWhere = utility.clean_dict()
                UpdateTemplateWhere['requirementid'] = int(requirementId)
                UpdateTemplateSet['reqFileDesc'] = data_text
                UpdateTemplateSet['isReqFileDescNew'] = 1
                DBSet = utility.clean_dict()
                DBSet['$set'] = UpdateTemplateSet
                custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager(
                ).STReqCollection, UpdateTemplateWhere, DBSet, connection)
                strtimestamp += ' ' + str(datetime.datetime.now())
                print(counttext)
                if filepath[-4:].lower() == ".doc":
                    docreadcount += 1
                    print('Total doc read count:' + str(docreadcount))
            print('File : ' + str(file_count) + ' ' + strtimestamp)
            print('Antiword empty count:' + str(antiwordemptycount))
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            # .encode('utf8'))
            utility.write_to_file(
                config.ConfigManager().LogFile, 'a', exception_message)
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'Number of req desc files read - ' + str(file_count) + ' ' + str(datetime.datetime.now()))


if __name__ == "__main__":

    file_paths = []
    directory_list = []
    directory_list = utility.string_to_array(
        config.ConfigManager().ReqDocDirectory, ',', directory_list)
    file_paths = filemanager.directory_iterate(directory_list)
    route_dataread(file_paths)
    utility.archive_content(
        file_paths, config.ConfigManager().ArchiveDirectory)
