#!/usr/bin/python3.4
import os
import phonenumbers
import re
from pymongo import MongoClient
import dcrconfig
import config
import urlmarker
import utility
import phonenumbers

cl = (MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().ExternalHost)))
db = cl[config.ConfigManager().DataCollectionDB]
configcol = db[config.ConfigManager().ConfigCollection]
urlExceptions = db[config.ConfigManager().urlExceptions]
configdocs = configcol.find({})
stCandidateCollection = db[config.ConfigManager().STCandidateCollection]

rcl = (MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().mongoDBHost)))
rdb = rcl[config.ConfigManager().RatesDB]
mspusers = rdb[config.ConfigManager().STMSPColl]
stclients = rdb[config.ConfigManager().STClientsColl]
maskingcoll = rdb[config.ConfigManager().MaskingColl]
extentionexceptions = rdb[config.ConfigManager().extensionExceptions]


def phone_date_filter(match):
    re1 = '((?:(?:[0-2]?\\d{1})|(?:[3][01]{1})))(?![\\d])'    # Day 1
    re2 = '(\\/)' # Any Single Character 1
    re3 = '((?:(?:[1]{1}\\d{1}\\d{1}\\d{1})|(?:[2]{1}\\d{3})))(?![\\d])'  # Year 1
    re4 = '(.)'   # Any Single Character 2
    re5 = '((?:(?:[0-2]?\\d{1})|(?:[3][01]{1})))(?![\\d])'    # Day 2
    re6 = '(\\/)' # Any Single Character 3
    re7 = '((?:(?:[1]{1}\\d{1}\\d{1}\\d{1})|(?:[2]{1}\\d{3})))(?![\\d])'  # Year 2

    rgexp1 = re.compile(re1+re2+re3+re4+re5+re6+re7,re.IGNORECASE|re.DOTALL)
    rgexp2 = re.compile(re3+re4+re5+re6+re7,re.IGNORECASE|re.DOTALL)
    rgexp_match1 = rgexp1.findall(str(match).split(")", 1)[1].replace(" ", ""))
    rgexp_match2 = rgexp2.findall(str(match).split(")", 1)[1].replace(" ", ""))
    if rgexp_match1 or rgexp_match2:
        isdate = True
    else:
        isdate = False
    return isdate


def string_to_array(string_object, delimiter, directory_list):
    directory_list = string_object.split(delimiter)
    return directory_list


def removephonenumber(text):
    phonePattern = re.compile("\d?(\(?\d{3}\D{0,3}\d{3}\D{0,3}\d{4})", re.S)
    # text = phonePattern.sub(lambda m: re.sub('\d', 'X', m.group(1)), text)
    text = phonePattern.sub(lambda m: re.sub('\d', '', m.group(1)), text)
    return text


def removephonenumber_new(text):
    country_codes = []
    country_codes = string_to_array(configdocs[0]['countryCodes'], ',', country_codes)
    for code in country_codes:
        matches = phonenumbers.PhoneNumberMatcher(text, code)
        for match in matches:
            isdate = phone_date_filter(match)
            if not isdate:
                text = utility.find_word_scrub(text, match.raw_string)
    return text


def removeemail(text):
    matches = re.findall(r'\b[^@^\s]+@[^@^\s]+\.[^@^\s]+\b', text)
    for match in matches:
        text = text.replace(match, "")
    return text


def fill_list_from_db_object(dbobjectcollection, fieldname):
    dbitems = []
    for dbobject in dbobjectcollection:
        dbitems.append(dbobject[fieldname])
    return dbitems


def convert_list_items_to_lower_case(inputlist):
    outputlist = [x.lower() for x in inputlist]
    return outputlist


def removeurl(text):
    urls = re.findall(urlmarker.WEB_URL_REGEX, text)
    if urls:
        url_exception_collection = urlExceptions.find()
        exceptionList = fill_list_from_db_object(url_exception_collection, 'url')
        exceptionList = convert_list_items_to_lower_case(exceptionList)
        urlsList = convert_list_items_to_lower_case(urls)
        if set(urlsList).issubset(set(exceptionList)):
            pass
        else:
            for url in urls:
                text = text.replace(url, "")
    return text


def removeclientname(text, clientID):
    if stclients.count({"clientID": int(clientID)}) > 0:
        clientData = stclients.find({"clientID": int(clientID)})
        cleanName = clientData[0]['Name']
        if 'synonyms' in clientData[0]:
            synonyms = clientData[0]['synonyms']
            synonyms = synonyms.split("|!@#$%|")
            synonyms.sort(key=len, reverse=True)
            for syn in synonyms:
                text = utility.find_word_scrub(text, syn)
        text = utility.find_word_scrub(text, cleanName)
    return text


def removemspusername(text, mspID):
    if mspusers.count({"mspID": int(mspID)}) > 0:
        mspUserData = mspusers.find({"mspID": int(mspID)})
        mspUserName = mspUserData[0]['Name']
        text = utility.find_word_scrub(text, mspUserName)
    return text


def removesuppliername(text, supplierName):
    extentions = extentionexceptions.find({})
    # extentions.sort(key=len, reverse=True)
    supplier = ''
    extensionremove = []
    for exe in extentions:
        supplier = supplierName+' '+exe['Name']
        text = utility.find_word_scrub(text, supplier)
        extensionremove.append(exe['Name'])

    text = utility.find_word_scrub(text, supplierName)
# ------------------------------------------------------------------------------
    # removing extension and searching for the supplier name
    for exe in extensionremove:
        supplier = utility.find_word_scrub(supplierName, exe)
        supplier = supplier.strip()
        text = utility.find_word_scrub(text, supplier)
# ------------------------------------------------------------------------------
    return text


def scrubtext(text, supplierName, clientID, mspID):
    text = removeemail(text)
    text = removephonenumber_new(text)
    text = removeurl(text)
    if supplierName != '':
        text = removesuppliername(text, supplierName)
    if mspID != '':
        text = removemspusername(text, mspID)
    if clientID != '':
        text = removeclientname(text, clientID)
    return text


def scrubcandidate(text, candidateId):
    # Not using projection as all documents may not have the projected items
    candidates = stCandidateCollection.find({'candidateid': int(candidateId)})
    if candidates.count() > 0:
        candidate = candidates[0]
        if 'firstName' in candidate:
            if candidate['firstName'] != '':
                text = utility.find_word_scrub(text, candidate['firstName'])
        if 'middleName' in candidate:
            if candidate['middleName'] != '':
                text = utility.find_word_scrub(text, candidate['middleName'])
        if 'lastName' in candidate:
            if candidate['lastName'] != '':
                text = utility.find_word_scrub(text, candidate['lastName'])
    return text
