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

topMatches = [
                [
                    [5960, "STTTM-REQ-0206", 129.7766571044921875],
                    [5960, "EPRI-REQ-0095", 116.3002090454101562]
                ],
                [
                    [10888, "MFC-REQ-0729", 117.4015502929687500],
                    [10888, "Caesars-REQ-0610", 104.6598510742187500]
                ],
                [
                    [11543, "MRT-REQ-0961", 101.0048675537109375],
                    [11543, "MRT-REQ-0965", 99.7555923461914062]
                ],
                [
                    [13888, "Caesars-REQ-0271", 81.4915771484375000],
                    [13888, "EBS-REQ-0469", 63.3295669555664062]
                ],
                [
                    [16174, "Caesars-REQ-0677", 150.3731536865234375],
                    [16174, "YP-REQ-1345", 132.3329315185546875]
                ],
                [
                    [18802, "Caesars-REQ-0472", 92.2651138305664062],
                    [18802, "Caesars-REQ-0710", 85.6282119750976562]
                ],
                [
                    [19433, "MFC-REQ-0729", 79.9669418334960938],
                    [19433, "DFS-REQ-05774", 61.4640998840332031]
                ],
                [
                    [21007, "EPRI-REQ-0176", 43.2437934875488281],
                    [21007, "Caesars-REQ-0472", 39.5384674072265625]
                ]
            ]
for firstList in topMatches:
    candidateId = ""
    outPut = ""
    for data in firstList:
        candidateId = data[0]
        reqId = data[1]
        matchIndex = data[2]
        print(data)
        canDetails = datacol.find_one({"applicationuserid": candidateId}, {"nounPhrases": 1, "description": 1, "resumeText": 1})
        reqDetails = stdatacol.find_one({"CLSRNumber": reqId}, {"nounPhrases": 1, "description": 1})
        outPut += "reqID : "+str(reqId)+" | CandidateId :"+str(candidateId)+" | MatchIndex :"+str(matchIndex)+str("\n")
        outPut += "--------------------------------------------------------------------------"+str("\n")
        outPut += "Candidate Description : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += canDetails['nounPhrases']+str("\n")
        outPut += canDetails['resumeText']+str("\n")
        outPut += "Requirement Description : "+str("\n")
        outPut += "-------------------------"+str("\n")
        # outPut += reqDetails['nounPhrases']+str("\n")
        outPut += reqDetails['description']+str("\n")
        outPut += "============================================================================"+str("\n")

    # f = open('/home/neeshu/development/txtFiles/'+str(str(candidateId)+"--"+str(reqId)+".txt"), 'w', newline='\n')
    f = open('/home/lshp/development/txtFiles/'+str(candidateId)+".txt", 'w', newline='\n')
    f.write(outPut)
    f.close()
