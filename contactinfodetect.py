#!/usr/bin/python3.4

import component
import utility
import config
import filemanager
import datetime
from pymongo import MongoClient
import dcrconfig
import os
import http.client
import json


utility.write_to_file(config.ConfigManager().LogFile,
                      'a', 'ST Supplier info detection running.' +
                      ' ' + str(datetime.datetime.now()))

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().resumesDetectDb]
collection = db[config.ConfigManager().fileDetectionsColl]
flagdetailscollection = db[config.ConfigManager().fileDetectionDetailsColl]
file = config.ConfigManager().LogFile


def sendtoexternalsystem():
    resumeData = collection.find({"isSent": 0}, {"_id": 0, "isSent": 0})
    candidateFlags = []
    for data in resumeData:
        data["fileName"] = " "
        candidateFlags.append(data)
    if candidateFlags:
        headers = {"Content-Type": "application/json"}
        candidateFlags = json.dumps(candidateFlags).encode('utf8')
        conn = http.client.HTTPConnection(config.ConfigManager().STwebApiHost, "80")
        conn.request(config.ConfigManager().JobServerMethod, config.ConfigManager().STwebApiUrl, candidateFlags, headers)
        response = conn.getresponse()
        try:
            if response.status == 200:
                data = response.read()
                result = json.loads(data.decode('utf8'))
                resumeIds = result[0]["resumeID"].split(',')
                resumeIds = [str(x.strip()) for x in resumeIds]
                print(resumeIds)
                if resumeIds:
                    collection.update({"batchId": {"$in": resumeIds}}, {"$set": {"isSent": 1}}, multi= True)
                    utility.write_to_file(config.ConfigManager().LogFile,
                                          'a', 'ST candidate resume screening, '+str(len(resumeIds))+' flag(s) sent successfully' +
                                          ' ' + str(datetime.datetime.now()))
                else:
                    utility.write_to_file(config.ConfigManager().LogFile,
                                          'a', 'ST candidate resume screening, no candidates found' +
                                          ' ' + str(datetime.datetime.now()))
            else:
                ex = str(response.status) + "--" + str(response.reason)
                utility.log_exception_file(ex, file)
                utility.write_to_file(config.ConfigManager().LogFile,
                                      'a', 'ST candidate resume screening, API down time' +
                                      ' ' + str(datetime.datetime.now()))
        except BaseException as ex:
            utility.log_exception_file(ex, file)


def sendflagdetailstoexternalsystem():
    flagdetailsdata = flagdetailscollection.find({"isSent": 0}, {"_id": 0, "isSent": 0})
    flagdetailsList = []
    for data in flagdetailsdata:
        flagdetailsList.append(data)
    if flagdetailsList:
        headers = {"Content-Type": "application/json"}
        flagdetailsList = json.dumps(flagdetailsList).encode('utf8')
        conn = http.client.HTTPConnection(config.ConfigManager().STwebApiHost, "80")
        conn.request(config.ConfigManager().JobServerMethod, config.ConfigManager().stWebApiSendData, flagdetailsList, headers)
        # conn = http.client.HTTPConnection("localhost", "4400")
        # conn.request(config.ConfigManager().JobServerMethod, config.ConfigManager().stWebApiSendData, flagdetailsList, headers)
        response = conn.getresponse()
        try:
            if response.status == 200:
                data = response.read()
                result = json.loads(data.decode('utf8'))
                # resumeIds = result[0]["resumeID"].split(',')
                resumeIds = [str(x.strip()) for x in result]
                if resumeIds:
                    flagdetailscollection.update({"batchId": {"$in": resumeIds}}, {"$set": {"isSent": 1}}, multi= True)
                    utility.write_to_file(config.ConfigManager().LogFile,
                                          'a', 'ST candidate resume screening, '+str(len(resumeIds))+' resume detection details sent successfully' +
                                          ' ' + str(datetime.datetime.now()))
                else:
                    utility.write_to_file(config.ConfigManager().LogFile,
                                          'a', 'ST candidate resume screening, no resume detection update done' +
                                          ' ' + str(datetime.datetime.now()))
            else:
                ex = str(response.status) + "--" + str(response.reason)
                utility.log_exception_file(ex, file)
                utility.write_to_file(config.ConfigManager().LogFile,
                                      'a', 'ST candidate resume screening, API down time' +
                                      ' ' + str(datetime.datetime.now()))
        except BaseException as ex:
            utility.log_exception_file(ex, file)


# flags order should be image,phone,email,url,supplier
def inserttodb(detection_dict_list):
    collection.insert(detection_dict_list)

flagDetailsColl = db[config.ConfigManager().fileDetectionDetailsColl]


def insertFlagsDetails(flagsDetails):
    flagDetailsColl.insert(flagsDetails)


def create_dict(flags, batchId, file_name):

    detection_dict = {}

    detection_dict["batchId"] = batchId
    detection_dict["fileName"] = file_name
    detection_dict["imageFlag"] = flags[0]
    detection_dict["phoneNumberFlag"] = flags[1]
    detection_dict["emailFlag"] = flags[2]
    detection_dict["urlFlag"] = flags[3]
    detection_dict["supplierNameFlag"] = flags[4]
    detection_dict["isSent"] = 0
    return detection_dict


def all_filepath_dict_list(detection_dict_list_all, batchId, file_name):
    finalFlags = [2, 2, 2, 2, 2]
    detection_dict_all = create_dict(finalFlags, batchId, file_name)
    detection_dict_all["isSent"] = 1
    detection_dict_list_all.append(detection_dict_all)
    return detection_dict_list_all


if __name__ == "__main__":
    filepaths = []
    directory_list = []
    # print(config.ConfigManager().fileDirectory)
    directory_list = utility.string_to_array(config.ConfigManager().fileDirectory, ',', directory_list)
    filepaths = filemanager.directory_iterate(directory_list)
    detection_dict_list = []
    detection_dict_list_all = []

    for filepath in filepaths:
        strtimestamp = str(datetime.datetime.now())
        data_text = ''
        # flags = []
        filepath_mod = filepath.replace('\\', '!@#$%')
        file_batchId_name = utility.filename_from_filepath(filepath_mod)
        batchIdsupplierId = (file_batchId_name[1:]).split('_')[0]
        batchId = (batchIdsupplierId).split('-')[0]
        supplierId = int((batchIdsupplierId).split('-')[1])
        file_name = (file_batchId_name[1:]).split('_')[1]
        file_name = file_name.replace('!@#$%', '\\')
        try:
            # file_count += 1
            # print(filepath)
            if filepath[-4:].lower() == ".txt":
                flagsInfo = component.read_text_text(filepath, supplierId)
                print(flagsInfo)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
            elif filepath[-4:].lower() == ".pdf":
                flagsInfo = component.read_pdf_text(filepath, supplierId)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                print(finalFlags)
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
            elif filepath[-5:].lower() == ".docx":
                flagsInfo = component.read_docx_text(filepath, supplierId)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
            elif filepath[-4:].lower() == ".odt":
                flagsInfo = component.read_odt_text(filepath, supplierId)
                print(flagsInfo)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
            elif filepath[-4:].lower() == ".rtf":
                flagsInfo = component.read_file_content(filepath, supplierId)
                print(flagsInfo)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
            elif filepath[-4:].lower() == ".doc":
                flagsInfo = component.read_doc_text_catdoc(filepath, supplierId)
                flags = flagsInfo[0]
                flagsDetails = flagsInfo[1]
                flagsDetailsBoolean = bool(flagsDetails)
                if flagsDetailsBoolean:
                    flagsDetails['batchId'] = batchId
                    flagsDetails["isSent"] = 0
                    insertFlagsDetails(flagsDetails)
                finalFlags = [j for i in flags for j in i]
                detection_dict = create_dict(finalFlags, batchId, file_name)
                detection_dict_list.append(detection_dict)
        except BaseException as ex:
            exception_message = '\n' + 'Exception:' + \
                str(datetime.datetime.now()) + '\n'
            exception_message += 'File: ' + '\n'
            exception_message += '\n' + str(ex) + '\n'
            exception_message += '-' * 100
            utility.write_to_file(config.ConfigManager().LogFile, 'a', exception_message)

        if not any(dict.get('batchId', None) == batchId for dict in detection_dict_list):
            detection_dict_list_all = all_filepath_dict_list(detection_dict_list_all, batchId, file_name)
        os.remove(filepath)

    if detection_dict_list:
        inserttodb(detection_dict_list)
    if detection_dict_list_all:
        inserttodb(detection_dict_list_all)

    sendtoexternalsystem()
    #------------------Need to stay commented out till corresponding functionality is done in ST------------------
    # sendflagdetailstoexternalsystem()
    #-------------------------------------------------------------------------------------------------------------
