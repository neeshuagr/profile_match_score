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


def scrubtext(text, supplierName, clientID, mspID, cleanUpListDict):
    # print(cleanUpListDict['configdocs'])
    # print(cleanUpListDict['urlExceptionsList'])
    # print(cleanUpListDict['extentions'])
    # print(cleanUpListDict['mspusersList'])
    # print(cleanUpListDict['stclientsList'])
    text = removeemail(text)
    text = removephonenumber_new(text, cleanUpListDict['configdocs'])
    text = removeurl(text, cleanUpListDict['urlExceptionsList'])
    if supplierName != '':
        text = removesuppliername(text, supplierName, cleanUpListDict['extentions'])
    if mspID != '':
        text = removemspusername(text, mspID, cleanUpListDict['mspusersList'])
    if clientID != '':
        text = removeclientname(text, clientID, cleanUpListDict['stclientsList'])
    return text


def scrubcandidate(text, candidateId, candidates):
    candidateList = [item for item in candidates if item['candidateid'] == int(candidateId)]
    if candidateList:
        candidate = candidateList[0]
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


def removeemail(text):
    matches = re.findall(r'\b[^@^\s]+@[^@^\s]+\.[^@^\s]+\b', text)
    for match in matches:
        text = text.replace(match, "")
    return text


def removephonenumber_new(text, configdocs):
    country_codes = []
    country_codes = string_to_array(configdocs[0]['countryCodes'], ',', country_codes)
    for code in country_codes:
        matches = phonenumbers.PhoneNumberMatcher(text, code)
        for match in matches:
            isdate = phone_date_filter(match)
            if not isdate:
                text = utility.find_word_scrub(text, match.raw_string)
    return text


def removeurl(text, urlExceptions):
    urls = re.findall(urlmarker.WEB_URL_REGEX, text)
    if urls:
        url_exception_collection = urlExceptions
        exceptionList = fill_list_from_db_object(url_exception_collection, 'url')
        exceptionList = convert_list_items_to_lower_case(exceptionList)
        urlsList = convert_list_items_to_lower_case(urls)
        if set(urlsList).issubset(set(exceptionList)):
            pass
        else:
            for url in urls:
                text = text.replace(url, "")
    return text


def removesuppliername(text, supplierName, extensions):
    supplier = ''
    extensionremove = []
    extentions = extensions
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


def removemspusername(text, mspID, mspusers):
    mspNames = [item['Name'] for item in mspusers if item['mspID'] == int(mspID)]
    if mspNames:
        mspUserName = mspNames[0]
        text = utility.find_word_scrub(text, mspUserName)
    return text


def removeclientname(text, clientID, stclients):
    clientNames = [item for item in stclients if item['clientID'] == int(clientID)]
    clientData = clientNames
    if clientData:
        cleanName = clientData[0]['Name']
        if 'synonyms' in clientData[0]:
            synonyms = clientData[0]['synonyms']
            synonyms = synonyms.split("|!@#$%|")
            synonyms.sort(key=len, reverse=True)
            for syn in synonyms:
                text = utility.find_word_scrub(text, syn)
        text = utility.find_word_scrub(text, cleanName)
    return text


def string_to_array(string_object, delimiter, directory_list):
    directory_list = string_object.split(delimiter)
    return directory_list


def fill_list_from_db_object(dbobjectcollection, fieldname):
    dbitems = []
    for dbobject in dbobjectcollection:
        dbitems.append(dbobject[fieldname])
    return dbitems


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


def convert_list_items_to_lower_case(inputlist):
    outputlist = [x.lower() for x in inputlist]
    return outputlist


def removephonenumber(text):
    phonePattern = re.compile("\d?(\(?\d{3}\D{0,3}\d{3}\D{0,3}\d{4})", re.S)
    # text = phonePattern.sub(lambda m: re.sub('\d', 'X', m.group(1)), text)
    text = phonePattern.sub(lambda m: re.sub('\d', '', m.group(1)), text)
    return text