#!/usr/bin/python3.4

import config
import utility
import custom
import datetime
import dbmanager


if __name__ == "__main__":

    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'currency data read running' + ' ' + str(datetime.datetime.now()))
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().RatesDB, config.ConfigManager().RatesConfigCollection)
    currencyID = int(configdocs[0]['currencyID'])
    currencyID = custom.master_data_transfer_from_sql(config.ConfigManager(
        ).STConnStr, config.ConfigManager().currencyQueryID, config.ConfigManager(
        ).currencyDetails, config.ConfigManager().ST, currencyID, config.ConfigManager().ExternalHost)

    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    if currencyID != 0:
        UpdateTemplateSet['currencyID'] = currencyID
    else:
        UpdateTemplateSet['currencyID'] = int(configdocs[0]['currencyID'])
    DBSet = utility.clean_dict()
    DBSet['$set'] = UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().RatesDB,
                                      config.ConfigManager().RatesConfigCollection, UpdateTemplateWhere, DBSet, connection)
