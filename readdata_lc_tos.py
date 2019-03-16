import component
import utility
import config
import filemanager
import datetime
from pymongo import MongoClient
import dcrconfig
import os
import http.client
import json
from xlrd import open_workbook

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().RatesDB]
LaborCategoryColl = db[config.ConfigManager().STlaborCateColl]

book = open_workbook(config.ConfigManager().laborCatetypeOfServicefile)
# ########## Labor Category ##############

sheet = book.sheet_by_index(0)

# read header values into the list
# sheet.ncols it gives all keys
keys = [sheet.cell(0, col_index).value for col_index in range(2)]

dict_list = []
for row_index in range(1, sheet.nrows):
    d = {keys[col_index]: sheet.cell(row_index, col_index).value
         # sheet.ncols it will gice all key values
         for col_index in range(2)}
    d["serviceMethodID"] = int(d["serviceMethodID"])
    d["LaborCategory"] = d.pop("Labor Category")
    dict_list.append(d)

dict_list = list({v['LaborCategory']: v for v in dict_list}.values())
LaborCategoryColl.insert(dict_list)
print("LC data inserted")

# ########## Type os Service ##############

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().RatesDB]
typeOfServiceColl = db[config.ConfigManager().STtypeofServiceColl]

sheet = book.sheet_by_index(1)

# read header values into the list
# sheet.ncols it gives all keys
keys = [sheet.cell(0, col_index).value for col_index in range(2)]

dict_list = []
for row_index in range(1, sheet.nrows):
    d = {keys[col_index]: sheet.cell(row_index, col_index).value
         # sheet.ncols it will gice all key values
         for col_index in range(2)}
    d["typeofServiceID"] = int(d["typeofServiceID"])
    dict_list.append(d)

dict_list = list({v['VMSTypeofService']: v for v in dict_list}.values())
typeOfServiceColl.insert(dict_list)
print("TOS data inserted")
