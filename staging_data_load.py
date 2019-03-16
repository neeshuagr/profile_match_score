#!/usr/bin/python3.4
#   Reading data from uploaded files and storing in to staging data base



import datetime
import config
import dbmanager
from pymongo import MongoClient
import dcrconfig
from xlrd import open_workbook
import utility
import sys
import os
import xlrd
import time


mandatoryFields = ["jobTitle", "jobDescription", "typeOfService", "country", "state", "city", "zipCode", "maxPayRate", "maxBillRate", "markUp", "currency", "dataSource"]
cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().RatesDB]
stTypeofServiceCollection = db[config.ConfigManager().STtypeofServiceColl]
stLaborCategoryCollection = db[config.ConfigManager().STlaborCateColl]
industryCollection = db[config.ConfigManager().ratesIndustryColl]
currencyCollection = db[config.ConfigManager().currecyColl]
stMspCollection = db[config.ConfigManager().STMSPColl]
stClientsCollection = db[config.ConfigManager().STClientsColl]
stagingCollection = db[config.ConfigManager().stagingCollection]


def staging_data_load(filepath):
    dataList = []
    errorList = []
    errorString = ""
    errorFinalString = ""
    excel_file = open_workbook(filepath)
    sheet = excel_file.sheets()[0]
    header_keys = [sheet.cell(0, colindex).value for colindex in range(sheet.ncols)]
    stTypeofServiceRows = [x for x in stTypeofServiceCollection.find({})]
    stLaborCategoryRows = [x for x in stLaborCategoryCollection.find({})]
    industryRows = [x for x in industryCollection.find({})]
    currencyRows = [x for x in currencyCollection.find({})]
    stClientRows = [x for x in stClientsCollection.find({})]
    stMspRows = [x for x in stMspCollection.find({})]
    yesNoRows = [{'yesNo': 'Yes'}, {'yesNo': 'No'}]
    geoCountryQuery = "select distinct name from geo_country order by name"
    geoStateQuery = "select distinct name from geo_admin1 order by name"
    geoCityQuery = "select distinct sPlaceName from GeoPostal order by sPlaceName"
    geoZipCodeQuery = "select distinct  sPostalCode from GeoPostal  order by sPostalCode"
    countryList = create_sql_data_list(geoCountryQuery, config.ConfigManager().geographicalDataConnstr, 'name')
    stateList = create_sql_data_list(geoStateQuery, config.ConfigManager().geographicalDataConnstr, 'name')
    cityList = create_sql_data_list(geoCityQuery, config.ConfigManager().geographicalDataConnstr, 'sPlaceName')
    zipCodeList = create_sql_data_list(geoZipCodeQuery, config.ConfigManager().geographicalDataConnstr, 'sPostalCode')
    for rowindex in range(1, sheet.nrows):
        try:
            errorString = ""
            row_dict = {header_keys[colindex]: sheet.cell(
                rowindex, colindex).value for colindex in range(sheet.ncols)}
            row_dict = add_fields(row_dict)
            mandatoryFieldsPresent = mandatory_fields_check(row_dict, mandatoryFields)
            if mandatoryFieldsPresent:
                mandatoryFieldsValuePresent = mandatory_fields_value_presence_check(row_dict, mandatoryFields)
                if mandatoryFieldsValuePresent:
                    stTypeofServiceValueAccuracyCheck = value_accuracy_check(row_dict, 'typeOfService', stTypeofServiceRows, "VMSTypeofService")
                    stLaborCategoryValueAccuracyCheck = value_accuracy_check(row_dict, 'laborCategory', stLaborCategoryRows, "LaborCategory")
                    industryValueAccuracyCheck = value_accuracy_check(row_dict, 'industry', industryRows, "IndustryName")
                    currencyValueAccuracyCheck = value_accuracy_check(row_dict, 'currency', currencyRows, "currencyCode")
                    clientValueAccuracyCheck = numerical_value_accuracy_check(row_dict, 'clientId', stClientRows, "clientID")
                    mspValueAccuracyCheck = numerical_value_accuracy_check(row_dict, 'mspId', stMspRows, "mspID")
                    rVFlagValueAccuracyCheck = value_accuracy_check(row_dict, 'remoteOrVirtualFlag', yesNoRows, "yesNo")
                    fpTFlagValueAccuracyCheck = value_accuracy_check(row_dict, 'fullTime', yesNoRows, "yesNo")
                    geoCountryAccuracyCheck = geo_data_check(row_dict, countryList, 'country')
                    geoStateAccuracyCheck = geo_data_check(row_dict, stateList, 'state')
                    geoCityAccuracyCheck = geo_data_check(row_dict, cityList, 'city')
                    geoZipCodeAccuracyCheck = geo_data_check(row_dict, zipCodeList, 'zipCode')
                    numericalValidationList = numerical_validation(row_dict, errorString)
                    errorString = numericalValidationList[1]
                    numericalValidation = numericalValidationList[0]
                    dateFormatValidation1 = post_date_format_check(row_dict, errorString, excel_file)
                    dateFormatValidation2 = post_date_format_check_two(row_dict, errorString)
                    if dateFormatValidation1 == False and dateFormatValidation2 == False:
                        dateFormatValidation = False
                        errorString += 'Post date is not in the right format; '
                    else:
                        dateFormatValidation = True
                    
                    if dateFormatValidation1:
                        row_dict['postDate'] = (datetime.datetime(*xlrd.xldate_as_tuple(row_dict['postDate'], excel_file.datemode))).date().isoformat()
                    if dateFormatValidation2:
                        row_dict['postDate'] = (datetime.datetime.strptime(str(row_dict['postDate']), '%Y-%m-%d')).date().isoformat()
                    if stTypeofServiceValueAccuracyCheck and stLaborCategoryValueAccuracyCheck and industryValueAccuracyCheck and currencyValueAccuracyCheck \
                       and geoCountryAccuracyCheck and geoStateAccuracyCheck and geoCityAccuracyCheck and geoZipCodeAccuracyCheck and clientValueAccuracyCheck\
                       and mspValueAccuracyCheck and numericalValidation and rVFlagValueAccuracyCheck and fpTFlagValueAccuracyCheck and dateFormatValidation:
                        dataList.append(row_dict)
                    else:
                        errorString = master_list_mismatch_message_composition(errorString, stTypeofServiceValueAccuracyCheck, stLaborCategoryValueAccuracyCheck, industryValueAccuracyCheck, currencyValueAccuracyCheck, clientValueAccuracyCheck, mspValueAccuracyCheck, rVFlagValueAccuracyCheck, fpTFlagValueAccuracyCheck, geoCountryAccuracyCheck, geoStateAccuracyCheck, geoCityAccuracyCheck, geoZipCodeAccuracyCheck)

                else:
                    errorString += 'Mandatory fields are empty; '
            else:
                errorString += 'Mandatory fields are absent; '
            errorString = error_string_clean(errorString)
            if not errorString == "":
                errorString = 'Errors in row ' + str(rowindex+1) + ' - ' + errorString
                errorList.append(errorString)
        except BaseException as ex:
            errorString = 'Errors in row ' + str(rowindex+1) + ' - ' + 'exception!!; ' + errorString
            errorList.append(errorString)
            utility.log_exception_file(ex, config.ConfigManager().LogFile)

    try:
        # file_back_up_and_removal(filepath)
        pass
    except BaseException as ex:
        utility.log_exception_file(ex, config.ConfigManager().LogFile)

    try:
        if dataList:
            insert_to_db(dataList, stagingCollection)
            if errorList:
                errorList.insert(0, 'Data submitted successfully! Please upload a brand new file after correcting the following errors.')
            else:
                errorList.insert(0, 'Data submitted successfully!')
        errorFinalString = '|!@#$%|'.join(errorList)
        print(errorFinalString)
    except BaseException as ex:
        print('Exception during data load!')
        utility.log_exception_file(ex, config.ConfigManager().LogFile)


def add_fields(row_dict):
    row_dict['userCreated'] = 'defaultUser'
    row_dict['userModified'] = 'defaultUser'
    row_dict['dateCreated'] = datetime.datetime.utcnow()
    row_dict['dateModified'] = datetime.datetime.utcnow()
    row_dict['source'] = 'fileUpload'
    return row_dict


def mandatory_fields_check(row_dict, mandatoryFields):
    mandatoryFieldsPresent = True
    keyList = list(row_dict.keys())
    if set(mandatoryFields).issubset(set(keyList)):
        pass
    else:
        print("The elements which are creating difference: ", set(mandatoryFields).difference(set(keyList)))
        mandatoryFieldsPresent = False
    return mandatoryFieldsPresent


def mandatory_fields_value_presence_check(row_dict, mandatoryFields):
    mandatoryFieldsValuePresent = True
    mandatoryFieldsValuePresentList = [False for x in mandatoryFields if row_dict[x] == '']
    if mandatoryFieldsValuePresentList:
        mandatoryFieldsValuePresent = False
    return mandatoryFieldsValuePresent


def value_accuracy_check(row_dict, keyValue, dictionaryList, masterDictKey):
    valueAccuracyCheck = False
    valueAccuracyCheckList = []
    if not row_dict[keyValue] == '':
        valueAccuracyCheckList = [True for x in dictionaryList if (row_dict[keyValue]).lower() == (x[masterDictKey]).lower()]
        if valueAccuracyCheckList:
            valueAccuracyCheck = True
    if keyValue == 'laborCategory' or keyValue == 'industry' or keyValue == 'remoteOrVirtualFlag' or keyValue == 'fullTime':
        if row_dict[keyValue] == '':
            valueAccuracyCheck = True
    return valueAccuracyCheck


def numerical_value_accuracy_check(row_dict, keyValue, dictionaryList, masterDictKey):
    valueAccuracyCheck = False
    valueAccuracyCheckList = []
    if not row_dict[keyValue] == '':
        valueAccuracyCheckList = [True for x in dictionaryList if row_dict[keyValue] == x[masterDictKey]]
        if valueAccuracyCheckList:
            valueAccuracyCheck = True
    if row_dict[keyValue] == '':
        valueAccuracyCheck = True
    return valueAccuracyCheck


def create_sql_data_list(query, connStr, keyName):
    cursor = dbmanager.cursor_odbc_connection(connStr)
    db_data_dict = dbmanager.cursor_execute(cursor, query)
    db_data = db_data_dict['dbdata']
    db_data_cursorexec = db_data_dict['cursor_exec']
    cursor_description = db_data_cursorexec.description
    column_headers = [column[0] for column in cursor_description]
    valueList = []
    valueList = [((dict(utility.zip_list(column_headers, row)))[keyName]).lower() for row in db_data]
    return valueList


def geo_data_check(row_dict, valueList, geoCode):
    geoDataMatch = False
    if geoCode == 'country':
        checkValue = row_dict['country']
    if geoCode == 'state':
        checkValue = row_dict['state']
    if geoCode == 'city':
        checkValue = row_dict['city']
    if geoCode == 'zipCode':
        checkValue = str(row_dict['zipCode'])
        checkValue = checkValue.strip()
        if checkValue[-2:] == '.0':
            checkValue = checkValue[:-2]
    valueList.append('notavailable')
    if (str(checkValue)).lower() in valueList:
        geoDataMatch = True
    return geoDataMatch


def master_list_mismatch_message_composition(errorString, stTypeofServiceValueAccuracyCheck, stLaborCategoryValueAccuracyCheck, industryValueAccuracyCheck, currencyValueAccuracyCheck, clientValueAccuracyCheck, mspValueAccuracyCheck, rVFlagValueAccuracyCheck, fpTFlagValueAccuracyCheck, geoCountryAccuracyCheck, geoStateAccuracyCheck, geoCityAccuracyCheck, geoZipCodeAccuracyCheck):
    if not stTypeofServiceValueAccuracyCheck:
        errorString += 'Type of service value does not match master data; '
    if not stLaborCategoryValueAccuracyCheck:
        errorString += 'Labor category value does not match master data; '
    if not industryValueAccuracyCheck:
        errorString += 'Industry value does not match master data; '
    if not currencyValueAccuracyCheck:
        errorString += 'Currency value does not match master data; '
    if not clientValueAccuracyCheck:
        errorString += 'Client Id value does not match master data; '
    if not mspValueAccuracyCheck:
        errorString += 'MSP Id value does not match master data; '
    if not rVFlagValueAccuracyCheck:
        errorString += 'remoteOrVirtualFlag should be empty or should have value Yes or No; '
    if not fpTFlagValueAccuracyCheck:
        errorString += 'fullTime should be empty or should have value Yes or No; '
    if not geoCountryAccuracyCheck:
        errorString += 'Country value does not match master data; '
    if not geoStateAccuracyCheck:
        errorString += 'State value does not match master data; '
    if not geoCityAccuracyCheck:
        errorString += 'City value does not match master data; '
    if not geoZipCodeAccuracyCheck:
        errorString += 'Zip code value does not match master data; '
    return errorString


def error_string_clean(errorString):
    if not errorString == '':
        errorString = errorString.strip()
        if errorString[len(errorString)-1] == ";":
            errorString = errorString[0:len(errorString)-1]
    return errorString


def numerical_validation(row_dict, errorString):
    numericalValidation = True

    try:
        float(row_dict['maxPayRate'])
        float(row_dict['maxBillRate'])
        if float(row_dict['maxPayRate']) < 0:
            numericalValidation = False
            errorString += 'maxPayRate is negative; '
        if float(row_dict['maxBillRate']) < 0:
            numericalValidation = False
            errorString += 'maxBillRate is negative; '
        if not row_dict['minPayRate'] == '':
            float(row_dict['minPayRate'])
            if row_dict['minPayRate'] > row_dict['maxPayRate']:
                numericalValidation = False
                errorString += 'minPayRate greater than maxPayRate; '
            if float(row_dict['minPayRate']) < 0:
                numericalValidation = False
                errorString += 'minPayRate is negative; '
        if not row_dict['minBillRate'] == '':
            float(row_dict['minBillRate'])
            if row_dict['minBillRate'] > row_dict['maxBillRate']:
                numericalValidation = False
                errorString += 'minBillRate greater than maxBillRate; '
            if float(row_dict['minBillRate']) < 0:
                numericalValidation = False
                errorString += 'minBillRate is negative; '
        if row_dict['maxPayRate'] > row_dict['maxBillRate']:
            numericalValidation = False
            errorString += 'maxPayRate greater than maxBillRate; '
    except BaseException as ex:
        numericalValidation = False
        errorString += 'One or more of the rates values are non empty and non numerical; '

    try:
        float(row_dict['markUp'])
        if float(row_dict['markUp']) < 0:
            errorString += 'markUp is negative; '
            numericalValidation = False
        if not row_dict['FICA'] == '':
            float(row_dict['FICA'])
            if float(row_dict['FICA']) < 0:
                errorString += 'FICA is negative; '
                numericalValidation = False
        if not row_dict['FUTA'] == '':
            float(row_dict['FUTA'])
            if float(row_dict['FUTA']) < 0:
                errorString += 'FUTA is negative; '
                numericalValidation = False
        if not row_dict['SUTA'] == '':
            float(row_dict['SUTA'])
            if float(row_dict['SUTA']) < 0:
                errorString += 'SUTA is negative; '
                numericalValidation = False
        if not row_dict['workersComp'] == '':
            float(row_dict['workersComp'])
            if float(row_dict['workersComp']) < 0:
                errorString += 'workersComp is negative; '
                numericalValidation = False
        if not row_dict['benefits'] == '':
            float(row_dict['benefits'])
            if float(row_dict['benefits']) < 0:
                errorString += 'benefits is negative; '
                numericalValidation = False
        if not row_dict['SGandA'] == '':
            float(row_dict['SGandA'])
            if float(row_dict['SGandA']) < 0:
                errorString += 'SGandA is negative; '
                numericalValidation = False
        if not row_dict['profit'] == '':
            float(row_dict['profit'])
            if float(row_dict['profit']) < 0:
                errorString += 'profit is negative; '
                numericalValidation = False
        if not row_dict['other'] == '':
            float(row_dict['other'])
            if float(row_dict['other']) < 0:
                errorString += 'other is negative; '
                numericalValidation = False
        if not row_dict['liabilityInsurance'] == '':
            float(row_dict['liabilityInsurance'])
            if float(row_dict['liabilityInsurance']) < 0:
                errorString += 'liabilityInsurance is negative; '
                numericalValidation = False
        if not row_dict['mandatorySickLeave'] == '':
            float(row_dict['mandatorySickLeave'])
            if float(row_dict['mandatorySickLeave']) < 0:
                errorString += 'mandatorySickLeave is negative; '
                numericalValidation = False
        if not row_dict['overhead'] == '':
            float(row_dict['overhead'])
            if float(row_dict['overhead']) < 0:
                errorString += 'overhead is negative; '
                numericalValidation = False
    except BaseException as ex:
        numericalValidation = False
        errorString += 'One or more of the mark up values are non empty and non numerical; '

    try:
        if not row_dict['mspId'] == '':
            if not float((row_dict['mspId'])).is_integer():
                numericalValidation = False
                errorString += 'Non integer value for mspId; '
        if not row_dict['clientId'] == '':
            if not float((row_dict['clientId'])).is_integer():
                numericalValidation = False
                errorString += 'Non integer value for clientId; '
        if float((row_dict['mspId'])) < 0 or float((row_dict['clientId'])) < 0:
            numericalValidation = False
            errorString += 'Negative value for clientId or mspId; '
    except BaseException as ex:
        numericalValidation = False
        errorString += 'Non integer value for clientId or mspId; '
    return numericalValidation, errorString


def post_date_format_check(row_dict, errorString, excel_file):
    dateFormatValidation = True
    try:
        postDateTimeOrig = datetime.datetime(*xlrd.xldate_as_tuple(row_dict['postDate'], excel_file.datemode))
        postDateOrig = postDateTimeOrig.date()
        time.strptime(str(postDateOrig), '%Y-%m-%d')
    except BaseException as ex:
        dateFormatValidation = False
    return dateFormatValidation


def post_date_format_check_two(row_dict, errorString):
    dateFormatValidation = True
    try:
        time.strptime(str(row_dict['postDate']), '%Y-%m-%d')
    except BaseException as ex:
        dateFormatValidation = False
    return dateFormatValidation


def insert_to_db(dataList, collection):
    collection.insert(dataList)


def file_back_up_and_removal(filepath):
    os.system("aws s3 cp " + filepath + " s3://dcr-analytics-backups/masterdatafileuploadbackup/ --profile default")
    os.remove(filepath)

if __name__ == "__main__":
    # fileName = 'RatesMasterData.xlsx'
    fileName = sys.stdin.readline()
    fileName = fileName.replace("\n", "")
    # filePath = "/home/neeshu/Downloads/Skype_Download/RatesMasterData.xlsx"
    filePath = config.ConfigManager().masterDataUploadPath + '/' + str(fileName)
    staging_data_load(filePath)
