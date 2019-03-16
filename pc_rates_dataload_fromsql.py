#!/usr/bin/python3.4

import config
import utility
import custom
import datetime
import dbmanager


if __name__ == "__main__":

    utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'PromptCloud data read from SQL' + ' '
                          + str(datetime.datetime.now()))


    custom.rates_data_from_Promptcloud_DB(config.ConfigManager().STPromptCloudConnStr,
                                          config.ConfigManager().
                                          STPromptcloudDataQueryId,
                                          config.ConfigManager().
                                          STPromptcloudDataDetails,
                                          config.ConfigManager().ST)
