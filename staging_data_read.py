#!/usr/bin/python3.4

import config
import dcrconfig
from pymongo import MongoClient
import data_cleanup_staging_to_master
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
import dcrgraph
import dcrnlp_with_generator


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


def dataclean(row, cleanUpListDict):
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
        jobTitle = data_cleanup_staging_to_master.scrubtext(jobTitle, supplierName, clientID, mspID, cleanUpListDict)
        jobDescription = data_cleanup_staging_to_master.scrubtext(jobDescription, supplierName, clientID, mspID, cleanUpListDict)
        mandatorySkills = data_cleanup_staging_to_master.scrubtext(mandatorySkills, supplierName, clientID, mspID, cleanUpListDict)
        desiredSkills = data_cleanup_staging_to_master.scrubtext(desiredSkills, supplierName, clientID, mspID, cleanUpListDict)
    if row['source'] == config.ConfigManager().fileUpload:
        jobTitle = data_cleanup_staging_to_master.scrubtext(jobTitle, supplierName, clientID, mspID, cleanUpListDict)
        jobDescription = data_cleanup_staging_to_master.scrubtext(jobDescription, supplierName, clientID, mspID, cleanUpListDict)
        mandatorySkills = data_cleanup_staging_to_master.scrubtext(mandatorySkills, supplierName, clientID, mspID, cleanUpListDict)
        desiredSkills = data_cleanup_staging_to_master.scrubtext(desiredSkills, supplierName, clientID, mspID, cleanUpListDict)
    if row['source'] == config.ConfigManager().ST:
        if jobTitle != '' and jobTitle is not None:
            jobTitle = data_cleanup_staging_to_master.scrubtext(jobTitle, supplierName, clientID, mspID, cleanUpListDict)
        if jobDescription != '' and jobDescription is not None:
            jobDescription = data_cleanup_staging_to_master.scrubtext(jobDescription, supplierName, clientID, mspID, cleanUpListDict)
        if 'resumeFlag' in row:
            if row['resumeFlag'] == 1:
                # Code review note - If parameter list was bundled into dictionary for scrubtext() then candidateID could have been passed to same function
                if description != '' and description is not None:
                    description = data_cleanup_staging_to_master.scrubcandidate(description, row['candidateid'], cleanUpListDict['stCandidateCollection'])
                if resumeText != '' and resumeText is not None:
                    resumeText = data_cleanup_staging_to_master.scrubcandidate(resumeText, row['candidateid'], cleanUpListDict['stCandidateCollection'])
        if mandatorySkills != '' and mandatorySkills is not None:
            mandatorySkills = data_cleanup_staging_to_master.scrubtext(mandatorySkills, supplierName, clientID, mspID, cleanUpListDict)
        if desiredSkills != '' and desiredSkills is not None:
            desiredSkills = data_cleanup_staging_to_master.scrubtext(desiredSkills, supplierName, clientID, mspID, cleanUpListDict)
        if description != '' and description is not None:
            description = data_cleanup_staging_to_master.scrubtext(description, supplierName, clientID, mspID, cleanUpListDict)
        row['description'] = description
        row['resumeText'] = resumeText
    row['jobTitle'] = jobTitle
    row['jobDescription'] = jobDescription
    row['mandatorySkills'] = mandatorySkills
    row['desiredSkills'] = desiredSkills
    if row['source'] != config.ConfigManager().promptCloud:
        row = datamasking(row)
    return row


def generatenounphrases(row):
    if row['source'] == config.ConfigManager().ST:
        description = row['description']
        noun_phrases = dcrnlp_with_generator.extract_nounphrases_sentences(description)
    elif row['source'] == config.ConfigManager().promptCloud:
        description = row['description']
        noun_phrases = dcrnlp_with_generator.extract_nounphrases_sentences(description)
    else:
        desc = str(row['jobTitle'])+' '+str(row['jobDescription'])+' '+str(row['mandatorySkills'])+' '+str(row['desiredSkills'])
        row['description'] = desc
        noun_phrases = dcrnlp_with_generator.extract_nounphrases_sentences(desc)
    row['nounPhrases'] = noun_phrases
    row['nounPhraseFlag'] = 1
    row['dateCreated'] = datetime.datetime.utcnow()
    row['dateModified'] = datetime.datetime.utcnow()
    return row


def signaturegraph(row, mapping_dict, edge_int_dict, neighborCount, diminition_percent):
    noun_phrases = row['nounPhrases']
    mapping_dict = mapping_dict
    edge_int_dict = edge_int_dict
    graph = dcrgraphcompactor.generate_document_signature_graph(
        mapping_dict,
        edge_int_dict,
        noun_phrases,
        neighborCount,
        diminition_percent
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


def data_cleanup_lists():
    cleanUpListDict = {}
    cl = (MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().ExternalHost)))
    db = cl[config.ConfigManager().DataCollectionDB]
    configcol = db[config.ConfigManager().ConfigCollection]
    urlExceptions = db[config.ConfigManager().urlExceptions]
    stCandidateCollection = db[config.ConfigManager().STCandidateCollection]

    rcl = (MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().mongoDBHost)))
    rdb = rcl[config.ConfigManager().RatesDB]
    mspusers = rdb[config.ConfigManager().STMSPColl]
    stclients = rdb[config.ConfigManager().STClientsColl]
    maskingcoll = rdb[config.ConfigManager().MaskingColl]
    extentionexceptions = db[config.ConfigManager().extensionExceptions]

    configdocs = list(configcol.find({}))
    urlExceptionsList = list(urlExceptions.find({}))
    extentions = list(extentionexceptions.find({}))
    mspusersList = list(mspusers.find({}))
    stclientsList = list(stclients.find({}))
    stCandidateCollection = list(stCandidateCollection.find({}, {'candidateid': 1, 'firstName': 1, 'middleName': 1, 'lastName': 1}))

    cleanUpListDict['configdocs'] = configdocs
    cleanUpListDict['urlExceptionsList'] = urlExceptionsList
    cleanUpListDict['extentions'] = extentions
    cleanUpListDict['mspusersList'] = mspusersList
    cleanUpListDict['stclientsList'] = stclientsList
    cleanUpListDict['stCandidateCollection'] = stCandidateCollection

    return cleanUpListDict


def readstagingdata():
    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'Staging dataread running' + ' ' + str(datetime.datetime.now()))
    ratesConfigValues = ratesConfig.find({})
    ratesDate = ratesConfigValues[0]['stagingDateModified']
    # ratesDataDateMax = ((stagingcoll.find().sort([('dateModified', -1)]).limit(1))[0])['dateModified']
    ratesDataCount = (stagingcoll.count({'dateModified': {"$gt": ratesDate}}))

    geoCountryQuery = "select distinct iso_alpha2, name, iso_alpha3, fips_code from geo_country order by name"
    # geoStateQuery = "select ga1.code, ga1.name, gn.admin1, gn.latitude, gn.longitude from geo_admin1 ga1 inner join geo_name gn on ga1.geonameid = gn.geonameid"
    geoStateQuery = "select gc.iso_alpha2, ga1.code, ga1.name, gn.admin1, gn.latitude, gn.longitude from geo_admin1 ga1 inner join geo_name gn on ga1.geonameid = gn.geonameid inner join geo_country gc on ltrim(rtrim(ga1.code)) like '%'+ ltrim(rtrim(gc.iso_alpha2))+'.' + '%'"
    geoCityQuery = "select distinct sAdminName1, sAdminCode1, sCountryCode, sPlaceName, fLatitude, fLongitude from GeoPostal order by sPlaceName"
    geoZipCodeQuery = "select distinct sAdminName1, sAdminCode1, sCountryCode, sPostalCode, fLatitude, fLongitude from GeoPostal  order by sPostalCode"
    # countryDictList = custom.create_sql_dict_list(geoCountryQuery, config.ConfigManager().geographicalDataConnstr)
    # stateDictList = custom.create_sql_dict_list(geoStateQuery, config.ConfigManager().geographicalDataConnstr)
    # cityDictList = custom.create_sql_dict_list(geoCityQuery, config.ConfigManager().geographicalDataConnstr)
    # zipCodeDictList = custom.create_sql_dict_list(geoZipCodeQuery, config.ConfigManager().geographicalDataConnstr)

    geoCountryDict = custom.create_geo_dict(geoCountryQuery, config.ConfigManager().geographicalDataConnstr, 'Country')
    geoStateDict = custom.create_geo_dict(geoStateQuery, config.ConfigManager().geographicalDataConnstr, 'State')
    geoCityDict = custom.create_geo_dict(geoCityQuery, config.ConfigManager().geographicalDataConnstr, 'City')
    geoZipCodeDict = custom.create_geo_dict(geoZipCodeQuery, config.ConfigManager().geographicalDataConnstr, 'zipCode')

    cleanUpListDict = data_cleanup_lists()

    ratesConfigValues = ratesConfig.find({})
    ratesDate = ratesConfigValues[0]['stagingDateModified']
    objectid = ratesConfigValues[0]['_id']
    lastDateTime = ratesConfigValues[0]['masterAutomationStartDate']
    oldDate = datetime.datetime.strptime(lastDateTime, '%Y-%m-%d')

    mapping_dict = dcrgraphcompactor.load_node_dict()
    edge_int_dict = dcrgraphcompactor.get_normalized_dictionary_from_int_edges()

    neighborCount = dcrgraph.neighbor_count_for_edge_weight()
    diminition_percent = dcrconfig.ConfigManager().DiminitionPercentage
    # while ratesDate < ratesDataDateMax:
    while ratesDataCount > 0:
        ratesConfigValues = ratesConfig.find({})
        ratesDate = ratesConfigValues[0]['stagingDateModified']
        # countTotalRecords = stagingcoll.count({'dateModified': {"$gt": ratesDate}})
        stepSize = int(config.ConfigManager().StagingMasterTransferStep)

        # if countTotalRecords < stepSize:
            # stepSize = countTotalRecords
        if ratesDataCount < stepSize:
            stepSize = ratesDataCount
        ratesDataCount = ratesDataCount - stepSize

        ratesData = stagingcoll.find({'dateModified': {"$gt": ratesDate}}, no_cursor_timeout=True).sort([('dateModified', 1)]).limit(int(config.ConfigManager().StagingMasterTransferStep))
        doc_id = ratesConfigValues[0]['masterDocId']
        dataList = []
        dateModifiedList = []

        i = 0
        for row in ratesData:
            try:

                dateModifiedList.append(row['dateModified'])
                i += 1
                print(i)
                del row['_id']
                doc_id += 1
                row['doc_id'] = doc_id
                # print('Start data clean ' + str(datetime.datetime.now()))
                # "Step:1 data scrubbing for email,phone,url and candidate name"
                row = dataclean(row, cleanUpListDict)
                # print('Start noun phrase gen ' + str(datetime.datetime.now()))
                # "Step:2 nounphrases generation"
                row = generatenounphrases(row)
                # print('Start signature graph ' + str(datetime.datetime.now()))
                # "Step:3 signature generation"
                row = signaturegraph(row, mapping_dict, edge_int_dict, neighborCount, diminition_percent)
                # print('Start rates calculation ' + str(datetime.datetime.now()))
                # "Step:4 rates calculation"
                row = rates_calculation.billratescalculation(row)
                # print('Start rate available ' + str(datetime.datetime.now()))
                # Put rate value calculation before this check
                # "Step:5 verification of rate availability"
                row = rate_available(row)
                # print('Start geo verify ' + str(datetime.datetime.now()))
                # geographical data check and additions
                row['iso_alpha2_value'] = ')(*&^'
                row['admin1_value'] = ')(*&^'
                row['state_name'] = ')(*&^'
                row = custom.geo_data_verify(row, geoCountryDict, 'country')
                row = custom.geo_data_verify(row, geoStateDict, 'state')
                row = custom.geo_data_verify(row, geoCityDict, 'city')
                row = custom.geo_data_verify(row, geoZipCodeDict, 'zipCode')
                del row['iso_alpha2_value']
                del row['admin1_value']
                del row['state_name']

                # print('Stop geo verify ' + str(datetime.datetime.now()))
                dataList.append(row)

            except BaseException as ex:
                utility.log_exception_file(ex, config.ConfigManager().LogFile)

        ratesData.close()
        del ratesData

        if dataList:
            # Step:4 insert data to db
            mastercoll.insert(dataList)
            doc_id = row['doc_id']

        todayDate = datetime.date.today()
        todayDate = datetime.datetime.strptime(str(todayDate), '%Y-%m-%d')
        delta = todayDate - oldDate
        days = delta.days

        if dateModifiedList:
            ratesDate = max(dateModifiedList)
        # Step:5 update config collection with doc_id and datetime
        updateconfigcollection(doc_id, ratesDate, objectid)

        if int(days) >= int(5):
            break

if __name__ == "__main__":

    readstagingdata()
