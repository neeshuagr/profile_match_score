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
        if dbrecordcount < 400000:
            for page in tree.getroot().findall('page'):
                dbrecordcount = job_info_analysis(page, filepath, dbrecordcount)

    # writing and printing variables
    write_variables()
    print_variables()


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


def job_info_analysis(page, filepath, dbrecordcount):
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict

    # Fetching current config paramters
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().
                                              MongoDBPort),
                                              config.ConfigManager().
                                              IntelligenceDb,
                                              config.ConfigManager().
                                              ConfigCollection)

    docid_count = int(configdocs[0]['docid_count'])
    dict_object_record_list = []
    for jobinfo in page.findall('record'):
                try:
                    # creating dictionary from xml tag contents
                    dict_object = utility.xml_to_dict(ET.tostring(jobinfo))
                    totaljobsdict = fill_job_by_site(filepath)
                    totalrecords += 1

                    # outer if check is jobdescription tag is in the xml
                    if 'jobdescription' in (dict_object['record']):
                        # checking if job description is none
                        if ((dict_object['record'])['jobdescription'] is not
                           None):
                            # variable to determine if record needs to be
                            # updated in DB
                            incorrectjobdescription = 0

                            # checking if job description is empty
                            if (((dict_object['record'])['jobdescription'])
                               .strip()) == '':
                                write_fileinfo(filepath, dict_object)
                                invalidrecords += 1
                                emptydesc += 1
                                incorrectjobdescription = 1
                                jobsitedict = fill_job_site_data(filepath)

                            # checking if job desc has less than 20 chars
                            if (len(((dict_object['record'])['jobdescription'])
                                    ) < 20):
                                incorrectjobdescription = 1
                                # eliminating the incomplete desc case
                                if (((dict_object['record'])['jobdescription'])
                                   .strip()[-3:]) == '...':
                                    print('Do nothing')
                                else:
                                    write_fileinfo(filepath, dict_object)
                                    invalidrecords += 1
                                    smalldesc += 1
                                    jobsitedict = fill_job_site_data(filepath)

                            # checking the incomplete desc case
                            if (((dict_object['record'])['jobdescription'])
                               .strip()[-3:]) == '...':
                                incorrectjobdescription = 1
                                write_fileinfo(filepath, dict_object)
                                invalidrecords += 1
                                incompletedesc += 1
                                jobsitedict = fill_job_site_data(filepath)

                            if (incorrectjobdescription == 0):
                                docid_count += 1
                                (dict_object['record'])['doc_id'] = docid_count
                                (dict_object['record'])['description'] = ((dict_object['record'])['jobdescription'])
                                (dict_object['record'])['nounPhrases'] = ""
                                dict_object_record_list.append(dict_object['record'])
                                dbrecordcount += 1

                        # checking if job description is none
                        if (dict_object['record'])['jobdescription'] is None:
                            write_fileinfo(filepath, dict_object)
                            invalidrecords += 1
                            nonedesc += 1
                            jobsitedict = fill_job_site_data(filepath)

                    else:
                        write_fileinfo(filepath, dict_object)
                        invalidrecords += 1
                        nodesc += 1
                        jobsitedict = fill_job_site_data(filepath)

                except BaseException as ex:
                    utility.log_exception_file(ex, dcrconfig.ConfigManager().SemanticGraphLogFile)
    if dict_object_record_list:
        insert_to_db(dict_object_record_list)
    # updating doc_id in config table
    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    UpdateTemplateSet['docid_count'] = docid_count
    DBSet = utility.clean_dict()
    DBSet['$set'] = UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort),
                                      config.ConfigManager().IntelligenceDb,
                                      config.ConfigManager().ConfigCollection,
                                      UpdateTemplateWhere, DBSet, connection)
    return dbrecordcount


def write_fileinfo(filepath, dict_object):
    filename = filepath.replace(config.ConfigManager().PCFileFolder + '/', '')
    FileInfo = filename + ', ' + (dict_object['record'])['uniq_id']
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', FileInfo)


def insert_to_db(dict_object_record_list):
    global connection
    custom.insert_data_to_DB_dBCollection(dict_object_record_list,
                                          config.ConfigManager().
                                          IntelligenceDataCollection, connection, config.ConfigManager().IntelligenceDb)


def write_variables():
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict

    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total invalid records: ' + str(invalidrecords))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total empty descriptions: ' + str(emptydesc))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total incomplete descriptions: ' +
                          str(incompletedesc))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total descriptions below 20 chars: ' +
                          str(smalldesc))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total null descriptions: ' + str(nonedesc))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total records without description tag: ' +
                          str(nodesc))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Invalid records per site: ')
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', str(jobsitedict))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total records: ' + str(totalrecords))
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', 'Total records per site: ')
    utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
                          'a', str(totaljobsdict))


def print_variables():
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict

    print(invalidrecords)
    print(emptydesc)
    print(incompletedesc)
    print(smalldesc)
    print(nonedesc)
    print(nodesc)
    print(jobsitedict)
    print(totalrecords)
    print(totaljobsdict)

if __name__ == "__main__":
    utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 3..! (pcdataload.py) ' + str(datetime.datetime.now()))
    file_paths = []
    directory_list = []
    directory_list = utility.string_to_array(
        config.ConfigManager().PCFileFolder, ',', directory_list)
    file_paths = filemanager.directory_iterate(directory_list)

    # analyzing xml
    analyze_data(file_paths)

    # capturing valid records
    valid_records()
    utility.archive_content(
        file_paths, config.ConfigManager().ArchiveDirectory)
