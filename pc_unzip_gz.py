#!/usr/bin/python3.4
#   Unzipping zipped files from Prompt Cloud


import gzip
import utility
import config
import filemanager
import datetime
import dcrconfig
import os


def route_compfileread(filepaths):
    for filepath in filepaths:
        try:
            # extracting data from .gz file.
            gzipfile = gzip.GzipFile(filepath, 'rb')
            gzipdata = gzipfile.read()
            gzipfile.close()

            # getting complete file name of the .gz file
            compfilename = utility.filename_from_filepath(filepath)
            # extracting the original file name
            filename = compfilename.split('.gz')[0]
            print(filename)

            # creating file and writing data
            uncompfile = open(config.ConfigManager(
            ).PCFileFolder + '/' + filename, 'wb')
            uncompfile.write(gzipdata)
            uncompfile.close()

        except BaseException as ex:
            utility.log_exception_with_filepath(ex, filepath)
            # writing to file the file names that cannot be extracted using
            # gzip
            utility.write_to_file(config.ConfigManager(
            ).PCDataAnalysisResultsFile, 'a', compfilename +
             '  cannot be extracted')
        os.remove(filepath)

if __name__ == "__main__":
    utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'promptcloud file unzip running' + ' ' + str(datetime.datetime.now()))
    compfile_paths = []
    compdirectory_list = []

    # putting all the folder names where file is downloaded to an array
    compdirectory_list = utility.string_to_array(
        config.ConfigManager().PCCompFolder, ',', compdirectory_list)
    # getting all the paths of files
    compfile_paths = filemanager.directory_iterate(compdirectory_list)
    route_compfileread(compfile_paths)
    # archiving and deleting files so that they don't get read again
    # utility.archive_content(
        # compfile_paths, config.ConfigManager().ArchiveDirectory)
