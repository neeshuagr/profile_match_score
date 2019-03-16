#!/usr/bin/python3.4

import config
import sys
from pymongo import MongoClient
import utility
import datetime
import dcrconfig
import http.client
import json

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().DataCollectionDB]
datacol = db[config.ConfigManager().xCHANGECandidateCollection]
stdatacol = db[config.ConfigManager().STReqCollection]
xchangematchcol = db[config.ConfigManager().XchangeMatchIndexColl]

outPut = ""
listData = []
topMatches = xchangematchcol.find({}).sort("matchIndex", -1).limit(50)
for data in topMatches:
    if data['candidateId'] not in listData:
        reqId = data['reqId']
        candidateId = data['candidateId']
        matchIndex = data['matchIndex']
        canDetails = datacol.find_one(
            {"applicationuserid": candidateId}, {"nounPhrases": 1, "description": 1})
        reqDetails = stdatacol.find_one(
            {"CLSRNumber": reqId}, {"nounPhrases": 1, "description": 1})
        outPut += "reqID : " + \
            str(reqId)+" | CandidateId :"+str(candidateId) + \
            " | MatchIndex :"+str(matchIndex)+str("\n")
        outPut += "-------------------------------------------------------" + \
            str("\n")
        outPut += "Candidate nounPhrases : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += canDetails['nounPhrases']+str("\n")
        outPut += canDetails['description']+str("\n")
        outPut += "Requirement nounPhrases : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += reqDetails['nounPhrases']+str("\n")
        outPut += reqDetails['description']+str("\n")
        outPut += "=======================================================" + \
            str("\n")
        listData.append(candidateId)

f = open(
    '/home/development/analytics.algo/Demo_MatchIndex.txt', 'w', newline='\n')
f.write(outPut)
f.close()

outPut += "+++++++++++++++++++ Low Matching +++++++++++++++++++++++++++++++" + \
    str("\n")

for canID in listData:
    topMatches = xchangematchcol.find(
        {"candidateId": canID}).sort("matchIndex", 1).limit(1)
    for data in topMatches:
        reqId = data['reqId']
        candidateId = data['candidateId']
        matchIndex = data['matchIndex']
        canDetails = datacol.find_one(
            {"applicationuserid": candidateId}, {"nounPhrases": 1, "description": 1})
        reqDetails = stdatacol.find_one(
            {"CLSRNumber": reqId}, {"nounPhrases": 1, "description": 1})
        outPut += "reqID : " + \
            str(reqId)+" | CandidateId :"+str(candidateId) + \
            " | MatchIndex :"+str(matchIndex)+str("\n")
        outPut += "-------------------------------------------------------" + \
            str("\n")
        outPut += "Candidate nounPhrases : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += canDetails['nounPhrases']+str("\n")
        outPut += canDetails['description']+str("\n")
        outPut += "Requirement nounPhrases : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += reqDetails['nounPhrases']+str("\n")
        outPut += reqDetails['description']+str("\n")
        outPut += "=======================================================" + \
            str("\n")

f = open(
    '/home/development/analytics.algo/Demo_MatchIndex.txt', 'w', newline='\n')
f.write(outPut)
f.close()
