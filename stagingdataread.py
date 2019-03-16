#!/usr/bin/python3.4

import config
import dcrconfig
from pymongo import MongoClient
import datacleanup
import dcrgraphcompactor
import dcrnlp
import utility
import dbmanager
import datetime
import custom
import http.client
import json
import sys
import rates_calculation


cl = MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().mongoDBHost))
db = cl[config.ConfigManager().RatesDB]
stagingcoll = db[config.ConfigManager().stagingCollection]
mastercoll = db[config.ConfigManager().masterCollection]
ratesConfig = db[config.ConfigManager().RatesConfigCollection]
file = config.ConfigManager().XchangeLogFile


# geoLocations = custom.getgeolocations(config.ConfigManager().GeoConnstr,
#                                       config.ConfigManager().
#                                       GeoDataFetchQueryId,
#                                       config.ConfigManager().
#                                       GeoDataFetchDetails,
#                                       config.ConfigManager().ST)


def makingjsondata(row):
    maskingText = {}
    if 'supplierName' in row and row['supplierName'] != '':
        supplierName = row['supplierName']
    else:
        supplierName = ''
    if 'clientId' in row and row['clientId'] != '':
        clientId = row['clientId']
    else:
        clientId = ''
    if 'mspId' in row and row['mspId'] != '':
        mspId = row['mspId']
    else:
        mspId = ''
    if 'dataSource' in row and row['dataSource'] != '':
        dataSource = row['dataSource']
    else:
        dataSource = ''
    # if 'source' in row and row['source'] != '':
    #     source = row['source']
    # else:
    #     source = ''
    maskingText['supplierName'] = supplierName
    maskingText['clientId'] = clientId
    maskingText['mspId'] = mspId
    maskingText['dataSource'] = dataSource
    # maskingText['source'] = source
    return maskingText


def datamasking(row):
    maskingText = makingjsondata(row)
    maskingText = json.dumps(maskingText)
    headers = {"Content-Type": "application/json"}
    conn = http.client.HTTPConnection(config.ConfigManager().Host, config.ConfigManager().Port)
    conn.request(config.ConfigManager().JobServerMethod, config.ConfigManager().API, maskingText, headers)
    response = conn.getresponse()
    data = response.read()
    result = json.loads(data.decode('utf8'))
    try:
        row['supplierName'] = result['supplierName']
        row['clientId'] = result['clientId']
        row['mspId'] = result['mspId']
        row['dataSource'] = result['dataSource']
        # row['source'] = result['source']
    except BaseException as ex:
        print(ex)
        utility.log_exception_file(ex, file)
    conn.close()
    return row


def dataclean(row):
    # masking = {}
    if 'jobTitle' in row and row['jobTitle'] != '':
        jobTitle = row['jobTitle']
    else:
        jobTitle = ''
    if 'jobDescription' in row and row['jobDescription'] != '':
        jobDescription = row['jobDescription']
    else:
        jobDescription = ''
    if 'mandatorySkills' in row and row['mandatorySkills'] != '':
        mandatorySkills = row['mandatorySkills']
    else:
        mandatorySkills = ''
    if 'desiredSkills' in row and row['desiredSkills'] != '':
        desiredSkills = row['desiredSkills']
    else:
        desiredSkills = ''
    if 'supplierName' in row and row['supplierName'] != '':
        supplierName = row['supplierName']
    else:
        supplierName = ''
    if 'clientId' in row and row['clientId'] != '':
        clientID = row['clientId']
    else:
        clientID = ''
    if 'mspId' in row and row['mspId'] != '':
        mspID = row['mspId']
    else:
        mspID = ''
    if 'description' in row and row['description'] != '':
        description = row['description']
    else:
        description = ''
    if 'resumeText' in row and row['resumeText'] != '':
        resumeText = row['resumeText']
    else:
        resumeText = ''
    if row['source'] == config.ConfigManager().uiFormPost:
        jobTitle = datacleanup.scrubtext(jobTitle, supplierName, clientID, mspID)
        jobDescription = datacleanup.scrubtext(jobDescription, supplierName, clientID, mspID)
        mandatorySkills = datacleanup.scrubtext(mandatorySkills, supplierName, clientID, mspID)
        desiredSkills = datacleanup.scrubtext(desiredSkills, supplierName, clientID, mspID)
    if row['source'] == config.ConfigManager().fileUpload:
        jobTitle = datacleanup.scrubtext(jobTitle, supplierName, clientID, mspID)
        jobDescription = datacleanup.scrubtext(jobDescription, supplierName, clientID, mspID)
        mandatorySkills = datacleanup.scrubtext(mandatorySkills, supplierName, clientID, mspID)
        desiredSkills = datacleanup.scrubtext(desiredSkills, supplierName, clientID, mspID)
    if row['source'] == config.ConfigManager().ST:
        if jobTitle != '' and jobTitle is not None:
            jobTitle = datacleanup.scrubtext(jobTitle, supplierName, clientID, mspID)
        if jobDescription != '' and jobDescription is not None:
            jobDescription = datacleanup.scrubtext(jobDescription, supplierName, clientID, mspID)
        if 'resumeFlag' in row:
            if row['resumeFlag'] == 1:
                # Code review note - If parameter list was bundled into dictionary for scrubtext() then candidateID could have been passed to same function
                if description != '' and description is not None:
                    description = datacleanup.scrubcandidate(description, row['candidateid'])
                if resumeText != '' and resumeText is not None:
                    resumeText = datacleanup.scrubcandidate(resumeText, row['candidateid'])
        if mandatorySkills != '' and mandatorySkills is not None:
            mandatorySkills = datacleanup.scrubtext(mandatorySkills, supplierName, clientID, mspID)
        if desiredSkills != '' and desiredSkills is not None:
            desiredSkills = datacleanup.scrubtext(desiredSkills, supplierName, clientID, mspID)
        if description != '' and description is not None:
            description = datacleanup.scrubtext(description, supplierName, clientID, mspID)
        row['description'] = description
        row['resumeText'] = resumeText
    row['jobTitle'] = jobTitle
    row['jobDescription'] = jobDescription
    row['mandatorySkills'] = mandatorySkills
    row['desiredSkills'] = desiredSkills
    row = datamasking(row)
    return row


def generatenounphrases(row):
    if row['source'] == config.ConfigManager().ST:
        description = row['description']
        noun_phrases = dcrnlp.extract_nounphrases_sentences(description)
    elif row['source'] == config.ConfigManager().promptCloud:
        description = row['description']
        noun_phrases = dcrnlp.extract_nounphrases_sentences(description)
    else:
        desc = str(row['jobTitle'])+' '+str(row['jobDescription'])+' '+str(row['mandatorySkills'])+' '+str(row['desiredSkills'])
        row['description'] = desc
        noun_phrases = dcrnlp.extract_nounphrases_sentences(desc)
    row['nounPhrases'] = noun_phrases
    row['nounPhraseFlag'] = 1
    row['dateCreated'] = datetime.datetime.utcnow()
    row['dateModified'] = datetime.datetime.utcnow()
    return row


def signaturegraph(row):
    noun_phrases = row['nounPhrases']
    mapping_dict = dcrgraphcompactor.load_node_dict()
    edge_int_dict = dcrgraphcompactor.get_normalized_dictionary()
    graph = dcrgraphcompactor.generate_document_graphs_from_dict_list_savetodb(
        mapping_dict,
        edge_int_dict,
        noun_phrases
        )
    row['signGraph'] = graph
    row['signGraphFlag'] = 1
    return row


def updateconfigcollection(docid, dateTime, whereID):
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    UpdateTemplateWhere['_id'] = whereID
    UpdateTemplateSet['masterDocId'] = docid
    UpdateTemplateSet['stagingDateModified'] = dateTime
    DBSet = utility.clean_dict()
    DBSet['$set'] = UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().RatesDB,
                                      config.ConfigManager().RatesConfigCollection, UpdateTemplateWhere, DBSet, connection)


def rate_available(row):
    if ('maxBillRate' in row and row['maxBillRate'] != '') or ('maxPayRate' in row and row['maxPayRate'] != ''):
        row['rateFlag'] = 1
    else:
        row['rateFlag'] = 0
    return row


def location_available(row):
    if 'city' in row and row['city'] != '' and row['city'] != "NotAvailable":
        row['locationFlag'] = 1
    else:
        row['locationFlag'] = 0
    return row


def get_lat_long_of_city(row):
    if 'city' in row and row['city'] != '' and row['city'] != "NotAvailable":
        for geo in geoLocations:
            if row['city'] == geo['sPlaceName']:
                row['fLatitude'] = geo['fLatitude']
                row['fLongitude'] = geo['fLongitude']
    return row


def readstagingdata():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'Staging dataread running' + ' ' + str(datetime.datetime.now()))
    ratesConfigValues = ratesConfig.find({})
    ratesDate = ratesConfigValues[0]['stagingDateModified']
    ratesData = stagingcoll.find({'dateModified': {"$gt": ratesDate}}, no_cursor_timeout=True)
    doc_id = ratesConfigValues[0]['masterDocId']
    objectid = ratesConfigValues[0]['_id']
    dataList = []
    dateModifiedList = []
    geoCountryQuery = "select distinct iso_alpha2, name, iso_alpha3, fips_code from geo_country order by name"
    geoStateQuery = "select ga1.code, ga1.name, gn.admin1, gn.latitude, gn.longitude from geo_admin1 ga1 inner join geo_name gn on ga1.geonameid = gn.geonameid"
    geoCityQuery = "select distinct sAdminName1, sAdminCode1, sCountryCode, sPlaceName, fLatitude, fLongitude from GeoPostal order by sPlaceName"
    geoZipCodeQuery = "select distinct sAdminName1, sAdminCode1, sCountryCode, sPostalCode, fLatitude, fLongitude from GeoPostal  order by sPostalCode"
    countryDictList = custom.create_sql_dict_list(geoCountryQuery, config.ConfigManager().geographicalDataConnstr)
    stateDictList = custom.create_sql_dict_list(geoStateQuery, config.ConfigManager().geographicalDataConnstr)
    cityDictList = custom.create_sql_dict_list(geoCityQuery, config.ConfigManager().geographicalDataConnstr)
    zipCodeDictList = custom.create_sql_dict_list(geoZipCodeQuery, config.ConfigManager().geographicalDataConnstr)
    i = 0
    for row in ratesData:
        try:
            dateModifiedList.append(row['dateModified'])
            i += 1
            del row['_id']
            doc_id += 1
            row['doc_id'] = doc_id

            "Step:1 data scrubbing for email,phone,url and candidate name"
            row = dataclean(row)

            "Step:2 nounphrases generation"
            row = generatenounphrases(row)

            "Step:3 signature generation"
            row = signaturegraph(row)

            "Step:4 rates calculation"
            row = rates_calculation.billratescalculation(row)

            # Put rate value calculation before this check
            "Step:5 verification of rate availability"
            row = rate_available(row)

            # "Step:5 verification of location/city availability"
            # row = location_available(row)

            # "Step:6 get lat long of city"
            # row = get_lat_long_of_city(row)

            # geographical data check and additions
            row['iso_alpha2_value'] = ')(*&^'
            row['admin1_value'] = ')(*&^'
            row['state_name'] = ')(*&^'
            row = custom.geo_data_check(row, countryDictList, 'country')
            row = custom.geo_data_check(row, stateDictList, 'state')
            row = custom.geo_data_check(row, cityDictList, 'city')
            row = custom.geo_data_check(row, zipCodeDictList, 'zipCode')
            del row['iso_alpha2_value']
            del row['admin1_value']
            del row['state_name']

            dataList.append(row)
            if i >= int(config.ConfigManager().StagingMasterTransferStep):
                "Step:4 insert data to db"
                mastercoll.insert(dataList)
                dataList = []
                i = 0
                docid = row['doc_id']
                stagingDateModified = max(dateModifiedList)
                "Step:5 update config collection with doc_id and datetime"
                updateconfigcollection(docid, stagingDateModified, objectid)

        except BaseException as ex:
            print(ex)
            utility.log_exception_file(ex, config.ConfigManager().LogFile)
            # utility.log_exception_file(row, config.ConfigManager().LogFile)

    ratesData.close()
    del ratesData

    # if dataList:
    #     "Step:4 insert data to db"
    #     mastercoll.insert(dataList)

        # docid = row['doc_id']
        # stagingDateModified = max(dateModifiedList)
        # "Step:5 update config collection with doc_id and datetime"
        # updateconfigcollection(docid, stagingDateModified, objectid)

if __name__ == "__main__":

    readstagingdata()
