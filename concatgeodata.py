#!/usr/bin/python3.4

import config
from pymongo import MongoClient
import dbmanager
import sys
import utility


cl = MongoClient(config.ConfigManager().MongoClient.replace("##host##", config.ConfigManager().mongoDBHost))
db = cl[config.ConfigManager().RatesDB]
mastercoll = db[config.ConfigManager().masterCollection]

print ("collection details :=> ",cl)

def modifygeodata():
    ratesData = mastercoll.find({})
    for row in ratesData:
        try:
            if row['cityLocationFlag'] == 1:
                cityGeoLocation = []
                cityGeoLocation.append(float(row['cityLongitude']))
                cityGeoLocation.append(float(row['cityLatitude']))
                row['coordinates'] = cityGeoLocation
                mastercoll.update({"doc_id": row['doc_id']}, {"$set": {"coordinates": cityGeoLocation}})
# if row['stateLocationFlag'] == 1:
#     stateGeoLocation = []
#     stateGeoLocation.append(float(row['stateLatitude']))
#     stateGeoLocation.append(float(row['stateLongitude']))
#     row['stateGeoLocation'] = stateGeoLocation
#     # print(row,"\n")
#     mastercoll.update({"doc_id": row['doc_id']},
#    {"$set": {"stateGeoLocation": stateGeoLocation}})

        except BaseException as ex:
            utility.log_exception_file(ex, config.ConfigManager().LogFile)


if __name__ == "__main__":

    modifygeodata()