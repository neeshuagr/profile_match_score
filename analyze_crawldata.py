#!/usr/bin/python3.4

import utility
import config
import filemanager
import datareadfiletypes
import datetime
import xml.etree.ElementTree as ET
import collections
from collections import OrderedDict
from pyexcel_ods3 import save_data
import os

orderedDict = collections.OrderedDict()
data = OrderedDict()


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
listdata_uniqueids = []
current_date = (str(datetime.datetime.now()).split(" "))[0]
analysis_file = config.ConfigManager().PCDataAnalysisResultsFile.replace('.ods', current_date + '.ods').replace('/mnt/nlpdata/','')


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

    # looping through file paths
    for filepath in filepaths:
        filecount += 1
        print(filepath)
        print('Processing file number: ' + str(filecount))

        # getting xml tree from file
        tree = datareadfiletypes.read_xml_tree(filepath)

        # drilling xml to get the job info tag contents
        for page in tree.getroot().findall('page'):
                job_info_analysis(page, filepath)
        os.remove(filepath)
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

    # data = OrderedDict()
    # data.update({"File unique id list": [[1, 2, 3], [4, 5, 6]]})
    # data.update({"Invalid records per site": [[]]})
    # data.update({"Valid records per site": [[]]})
    # save_data("pcdataanalysisresults.ods", data)
    # subtracting dictionary key values to get valid records per site
    validjobsdict = {key: totaljobsdict[key] - jobsitedict.get(key, 0)
                     for key in totaljobsdict.keys()}
    # '''utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total valid records per site: ')
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', str(validjobsdict))'''
    listdata = []
    sublist = []
    for val in validjobsdict:
        sublist = [val, validjobsdict[val]]
        listdata.append(sublist)
    data.update({"Valid records per site": listdata})
    save_data(analysis_file, data)


def job_info_analysis(page, filepath):
    global totalrecords
    global invalidrecords
    global emptydesc
    global incompletedesc
    global smalldesc
    global nonedesc
    global nodesc
    global totaljobsdict
    global jobsitedict

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
                            # checking if job description is empty
                            if (((dict_object['record'])['jobdescription'])
                               .strip()) == '':
                                write_fileinfo(filepath, dict_object)
                                invalidrecords += 1
                                emptydesc += 1
                                jobsitedict = fill_job_site_data(filepath)

                            # checking if job desc has less than 20 chars
                            if (len(((dict_object['record'])['jobdescription'])
                                    ) < 20):
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
                                write_fileinfo(filepath, dict_object)
                                invalidrecords += 1
                                incompletedesc += 1
                                jobsitedict = fill_job_site_data(filepath)

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
                    utility.log_exception_with_filepath(ex, filepath)


def write_fileinfo(filepath, dict_object):
    filename = filepath.replace(config.ConfigManager().PCFileFolder + '/', '')
    # FileInfo = filename + ', ' + (dict_object['record'])['uniq_id']
    # print(FileInfo)
#     listdata = []
    sublist = []
    sublist = [filename, (dict_object['record'])['uniq_id']]
    listdata_uniqueids.append(sublist)
    
    # save_data(config.ConfigManager().PCDataAnalysisResultsFile, data)
    # '''utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', FileInfo)'''


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

    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total invalid records: ' + str(invalidrecords))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total empty descriptions: ' + str(emptydesc))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total incomplete descriptions: ' +
    #                       str(incompletedesc))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total descriptions below 20 chars: ' +
    #                       str(smalldesc))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total null descriptions: ' + str(nonedesc))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total records without description tag: ' +
    #                       str(nodesc))
    # '''utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Invalid records per site: ')
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', str(jobsitedict))'''
    listdata = []
    sublist = []
    for val in jobsitedict:
        sublist = [val, jobsitedict[val]]
        listdata.append(sublist)
    data.update({"Invalid records per site": listdata})
    # '''utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total records: ' + str(totalrecords))
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', 'Total records per site: ')
    # utility.write_to_file(config.ConfigManager().PCDataAnalysisResultsFile,
    #                       'a', str(totaljobsdict))'''
    listdata = []
    sublist = []
    for val in totaljobsdict:
        sublist = [val, totaljobsdict[val]]
        listdata.append(sublist)
    data.update({"Total records per site": listdata})
    # /mnt/nlpdata/pcdataanalysisresults.ods or
    # config.ConfigManager().PCDataAnalysisResultsFile
    data.update({"File unique id list": listdata_uniqueids})
    save_data(analysis_file, data)


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
    utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'crawl data analysis running'+ ' ' + str(datetime.datetime.now()))
    file_paths = []
    directory_list = []
    directory_list = utility.string_to_array(
        config.ConfigManager().PCFileFolder, ',', directory_list)
    file_paths = filemanager.directory_iterate(directory_list)
    #os.system("echo 'b' | sudo -S touch " + analysis_file)
    #os.system("echo 'b' | sudo -S chmod 777 " + analysis_file)
    #os.system("echo 'b' | sudo -S chown neeshu:neeshu " + analysis_file)
    # os.system("echo 'b' | sudo -S cp " + analysis_file +" /mnt/nlpdata")
    # analyzing xml
    analyze_data(file_paths)

    # capturing valid records
    valid_records()
    os.system("echo 'b' | sudo -S cp " + analysis_file +" /mnt/nlpdata")
    os.system("rm " + analysis_file)
    # utility.archive_content(
        # file_paths, config.ConfigManager().ArchiveDirectory)
