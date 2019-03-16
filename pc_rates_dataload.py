#!/usr/bin/python3.4
#   Loading data to database


import utility
import config
import filemanager
import datareadfiletypes
import datetime
import xml.etree.ElementTree as ET
import dbmanager
import dcrnlp
import custom
import dcrconfig
import os
import sys
# global variable declaration
totalrecords = 0
invalidrecords = 0
emptydesc = 0
incompletedesc = 0
smalldesc = 0
nonedesc = 0
nodesc = 0
totaljobsdict = utility.clean_dict()
jobsitedict = utility.clean_dict()
connection = dbmanager.mongoDB_connection(int(config.ConfigManager().
                                          MongoDBPort))


def analyze_data(filepaths):
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict
    filecount = 0
    dbrecordcount = 0
    # looping through file paths
    for filepath in filepaths:
        filecount += 1
        print(filepath)
        print('Processing file number: ' + str(filecount))

        # getting xml tree from file
        tree = datareadfiletypes.read_xml_tree(filepath)
        # drilling xml to get the job info tag contents
        if config.ConfigManager().PromptCloudRecordLimitSet == "Yes":
            if dbrecordcount < int(config.ConfigManager().PromptCloudRecordLimit):
                for page in tree.getroot().findall('page'):
                    # dbrecordcount = job_info_analysis(page, filepath, dbrecordcount)
                    page_dict_object = utility.xml_to_dict(ET.tostring(page))
                    dbrecordcount = pc_rates_data_storage(page_dict_object, filepath, dbrecordcount)
        print(str(datetime.datetime.now()))
        os.remove(filepath)
    # writing and printing variables


def fill_job_site_data(filepath):
    global jobsitedict
    # site name is the string before _ in the file name
    site = (utility.filename_from_filepath(filepath)).split('_')[0]

    # condition to add dictionary key as site name and incrementing
    if site not in jobsitedict:
        jobsitedict[site] = 1
    else:
        jobsitedict[site] += 1
    return jobsitedict


def fill_job_by_site(filepath):
    global totaljobsdict
    # site name is the string before _ in the file name
    site = (utility.filename_from_filepath(filepath)).split('_')[0]

    # condition to add dictionary key as site name and incrementing
    if site not in totaljobsdict:
        totaljobsdict[site] = 1
    else:
        totaljobsdict[site] += 1
    return totaljobsdict


def valid_records():
    global totaljobsdict
    global jobsitedict

    # subtracting dictionary key values to get valid records per site
    validjobsdict = {key: totaljobsdict[key] - jobsitedict.get(key, 0)
                     for key in totaljobsdict.keys()}
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total valid records per site: ')
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', str(validjobsdict))


def job_info_analysis_storage(page_dict_object, filepath, dbrecordcount):
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict

    dict_object_record_list = []
    try:
        dict_object = page_dict_object['page']
        # outer if check is jobdescription tag is in the xml
        if 'jobdescription' in (dict_object['record']):
            # checking if job description is none
            if ((dict_object['record'])['jobdescription'] is not None):

                incorrectjobdescription = 0

                if (((dict_object['record'])['jobdescription']).strip()) == '':
                    incorrectjobdescription = 1

                if (len(((dict_object['record'])['jobdescription'])
                        ) < 20):
                    incorrectjobdescription = 1

                if (((dict_object['record'])['jobdescription']).strip()[-3:]) == '...':
                    incorrectjobdescription = 1

                if (incorrectjobdescription == 0):
                    (dict_object['record'])['dateCreated'] = datetime.datetime.now()
                    (dict_object['record'])['dateModified'] = datetime.datetime.now()
                    (dict_object['record'])['createdUser'] = 'defaultUser'
                    (dict_object['record'])['modifiedUser'] = 'defaultUser'
                    (dict_object['record'])['source'] = 'PromptCloud'
                    (dict_object['record'])['Url'] = dict_object['pageurl']
                    (dict_object['record'])['fileName'] = filepath.replace(config.ConfigManager().PCRatesFileFolder + '/', '')
                    dict_object_record_list.append(dict_object['record'])
                    dbrecordcount += 1

    except BaseException as ex:
        utility.log_exception_file(ex, dcrconfig.ConfigManager().SemanticGraphLogFile)
    if dict_object_record_list:
        insert_to_db(dict_object_record_list)
    # updating doc_id in config table

    return dbrecordcount


def pc_rates_data_storage(page_dict_object, filepath, dbrecordcount):
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict
    dict_object_record_list = []
    try:
        page_object_list = page_dict_object['page']
        if isinstance(page_object_list['record'], list):
            for record_object in page_object_list['record']:
                record_object = pc_rates_add_fields(record_object, filepath)
                if sys.getsizeof(record_object['description']) < 13000000:
                    dict_object_record_list.append(record_object)
                dbrecordcount += 1
        else:
            record_object = page_object_list['record']
            record_object = pc_rates_add_fields(record_object, filepath)

            if sys.getsizeof(record_object['description']) < 13000000:
                dict_object_record_list.append(record_object)
            dbrecordcount += 1
    except BaseException as ex:
        utility.log_exception_file(ex, config.ConfigManager().PromptcloudLogFile)
    if dict_object_record_list:
        insert_to_db(dict_object_record_list)
    # updating doc_id in config table

    return dbrecordcount


def write_fileinfo(filepath, dict_object):
    filename = filepath.replace(config.ConfigManager().PCFileFolder + '/', '')
    FileInfo = filename + ', ' + (dict_object['record'])['uniq_id']
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', FileInfo)


def pc_rates_add_fields(record_object, filepath):
    record_object['dateCreated'] = datetime.datetime.utcnow()
    record_object['dateModified'] = datetime.datetime.utcnow()
    record_object['createdUser'] = 'defaultUser'
    record_object['modifiedUser'] = 'defaultUser'
    record_object['source'] = config.ConfigManager().promptCloud
    record_object['fileName'] = filepath.replace(config.ConfigManager().PCRatesFileFolder + '/', '')
    record_object['description'] = ''
    # if 'rate_value' in record_object and record_object['rate_value'] != '':
    #     record_object['maxBillRate'] = record_object['rate_value']

    if 'job_description' in record_object and record_object['job_description'] != '':
        record_object['jobDescription'] = record_object['job_description']
        record_object['description'] = ''
        record_object['description'] += record_object['jobDescription']
        if 'jobtitle' in record_object and record_object['jobtitle'] != '':
            record_object['jobTitle'] = record_object['jobtitle']
            record_object['description'] += record_object['jobTitle']
    if 'jobdescription' in record_object and record_object['jobdescription'] != '':
        record_object['jobDescription'] = record_object['jobdescription']
        record_object['description'] = ''
        record_object['description'] += record_object['jobDescription']
        if 'jobtitle' in record_object and record_object['jobtitle'] != '':
            record_object['jobTitle'] = record_object['jobtitle']
            record_object['description'] += record_object['jobTitle']
        if 'skills' in record_object and record_object['skills'] != '':
            record_object['description'] += record_object['skills']

    if 'postdate' in record_object and record_object['postdate'] != '':
        record_object['postDate'] = record_object['postdate']
    else:
        record_object['postDate'] = datetime.datetime.today().strftime('%Y-%m-%d')

    if 'sitename' in record_object and record_object['sitename'] != '':
        record_object['dataSource'] = record_object['sitename']
    if 'site_name' in record_object and record_object['site_name'] != '':
        record_object['dataSource'] = record_object['site_name']
    return record_object


def insert_to_db(dict_object_record_list):
    # dummy collection PCRatesDataColl
    global connection
    custom.insert_data_to_DB_dBCollection(dict_object_record_list,
                                          config.ConfigManager().
                                          stagingCollection, connection, config.ConfigManager().RatesDB)


if __name__ == "__main__":
    print(str(datetime.datetime.now()))
    utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a',
        'PC rates data load' + str(datetime.datetime.now()))
    file_paths = []
    directory_list = []
    directory_list = utility.string_to_array(
        config.ConfigManager().PCRatesFileFolder, ',', directory_list)
    file_paths = filemanager.directory_iterate(directory_list)
    # analyzing xml
    analyze_data(file_paths)

    # capturing valid records
    # valid_records()
    # utility.archive_content(
    #     file_paths, config.ConfigManager().ArchiveDirectory)
    print(str(datetime.datetime.now()))
