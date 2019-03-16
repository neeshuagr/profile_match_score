#!/usr/bin/python3.4
#   Downloading zipped files from Prompt Cloud
#

import json
import urllib.request
from queue import Queue
from threading import Thread
import config
import time
from datetime import datetime
import custom
import dictionaries
import dbmanager


q = Queue()


def retrive_file(url):
    urllib.request.URLopener().retrieve(
        url, config.ConfigManager().PCCompFolder + '/' + url.split('/')[-1])
    print('Downloaded : ' + url.split('/')[-1])


def worker():
    # fetching items from queue
    while True:
        item = q.get()
        retrive_file(item)
        q.task_done()


def update_DB(configdocs, latestdate):
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    dictionaries.UpdateTemplateSet = {}
    dictionaries.UpdateTemplateWhere = {}
    dictionaries.UpdateTemplateSet['PClastDate'] = latestdate
    dictionaries.UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort),
                                      config.ConfigManager().IntelligenceDb,
                                      config.ConfigManager().ConfigCollection,
                                      dictionaries.UpdateTemplateWhere,
                                      dictionaries.DBSet,
                                      connection)

if __name__ == "__main__":
    # nounphrase_generate()
    # generating 15 threads and redirecting to worker function
    for i in range(15):
        t = Thread(target=worker)
        t.daemon = True
        t.start()
    # fetching promptcloud url and url response xml data
    url = config.ConfigManager().PromptCloudURL
    response = urllib.request.urlopen(url)
    print(response)
    data = json.loads(response.read().decode(
        response.info().get_param('charset') or 'utf-8'))

    urlcount = 0
    # configuration data from db
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager()
                                              .MongoDBPort),
                                              config.ConfigManager()
                                              .IntelligenceDb,
                                              config.ConfigManager()
                                              .ConfigCollection)
    print(configdocs[0])
    for item in data['root']['entry']:
        # for each entry taking th updated time
        updatets = item['updated_ts'].replace(' +0000', '')
        pclstdt = configdocs[0]['PClastDate']
        print(pclstdt)
        updatets_formatted = datetime.strptime(updatets, "%Y-%m-%d %H:%M:%S")
        pclastdate_formatted = datetime.strptime(pclstdt, "%Y-%m-%d %H:%M:%S")

        # filling queue with url if updated time > last date of download
        if (updatets_formatted > pclastdate_formatted):
            urlcount += 1
            q.put(item['url'])
            print(updatets_formatted)
            print(pclastdate_formatted)

    print(urlcount)

    # updating configuration collection with latest time
    update_DB(configdocs, data['root']['entry'][0]
              ['updated_ts'].replace(' +0000', ''))
    q.join()
    print("Download complete")
