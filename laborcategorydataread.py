#!/usr/bin/python3.4

import config
import utility
import custom
import datetime
import dbmanager


if __name__ == "__main__":

    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'st labor category data read running' + ' ' + str(datetime.datetime.now()))
    connection = dbmanager.mongoDB_connection(
        int(config.ConfigManager().MongoDBPort))
    configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager(
    ).MongoDBPort), config.ConfigManager().RatesDB, config.ConfigManager().RatesConfigCollection)
    laborCatagory = int(configdocs[0]['laborCatagory'])
    laborCatagory = custom.data_from_sqlDB_toMongo(config.ConfigManager(
        ).STConnStr, config.ConfigManager().STlaborCatagoryQueryid, config.ConfigManager(
        ).STlaborCatagoryDetails, config.ConfigManager().ST, laborCatagory)

    UpdateTemplateWhere = utility.clean_dict()
    UpdateTemplateSet = utility.clean_dict()
    UpdateTemplateWhere['_id'] = configdocs[0]['_id']
    if laborCatagory != 0:
        UpdateTemplateSet['laborCatagory'] = laborCatagory
    else:
        UpdateTemplateSet['laborCatagory'] = int(configdocs[0]['laborCatagory'])
    DBSet = utility.clean_dict()
    DBSet['$set'] = UpdateTemplateSet
    custom.update_data_to_Db_noupsert(int(config.ConfigManager().MongoDBPort), config.ConfigManager().RatesDB,
                                      config.ConfigManager().RatesConfigCollection, UpdateTemplateWhere, DBSet, connection)
