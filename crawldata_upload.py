#!/usr/bin/python3.4
import utility
import config
import datareadfiletypes
import filemanager
import datetime
import xml.etree.ElementTree as ET
import dbmanager
import dcrnlp
from queue import Queue
from threading import Thread

q = Queue()
fc = 0
count = 0


def load_data(filepath):
    connection = dbmanager.mongoDB_connection(27017)
    data_base = dbmanager.db_connection(connection, 'datacollectiondb')
    global fc
    global count

    fc += 1
    fcount = fc
    print("starting : " + str(fcount) + ":" + filepath)
    tree = datareadfiletypes.read_xml_tree(filepath)
    for page in tree.getroot().findall('page'):
        for jobinfo in page.findall('record'):
            try:
                count += 1
                status = "{:<8}".format(str(count)) + " :"
                status += "{:<4}".format(str(fcount)) + " :"
                status += str(datetime.datetime.now())
                dict_object = utility.xml_to_dict(ET.tostring(jobinfo))
                if 'jobdescription' in (dict_object['record']):
                    dict_object['record']['description'] = dict_object[
                        'record']['jobdescription']
                    dict_object['record']['nounPhrases'] = dcrnlp.extract_nounphrases_sentences(
                        str(dict_object['record']['description']))
                    status += str(datetime.datetime.now())
                    dbmanager.crud_create(
                        data_base, 'careerbuilderdata', dict_object['record'])
                    status += str(datetime.datetime.now())

            except BaseException as ex:
                exception_message = '\n' + 'Exception:' +\
                    str(datetime.datetime.now()) + '\n'
                exception_message += 'File: ' + filepath + '\n'
                exception_message += '\n' + str(ex) + '\n'
                exception_message += '-' * 100
                utility.write_to_file(
                    config.ConfigManager().LogFile, 'a', exception_message)
            print(status)


def worker():
    while True:
        item = q.get()
        load_data(item)
        q.task_done()

if __name__ == "__main__":

    for i in range(12):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    file_paths = []
    directory_list = ['/home/vinay/Documents/PCData/cdata/CareerBuilder']
    file_paths = filemanager.directory_iterate(directory_list)

    filecount = 0
    for filepath in file_paths:
        filecount += 1
        q.put(filepath)

    print(filecount)
    q.join()
    print("Download complete")
