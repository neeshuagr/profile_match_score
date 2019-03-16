#!/usr/bin/python3.4
#   File transfer.
#   Runs shell script file to transfer files


import subprocess
import os
import config
import utility
import dcrconfig


if __name__ == "__main__":
    semanticGraph = dcrconfig.ConfigManager().SemanticGraphFile.replace(config.ConfigManager().mountDirectory + '/', '')
    intEdges = dcrconfig.ConfigManager().IntegerEdegesFile.replace(config.ConfigManager().mountDirectory + '/', '')
    nodeDict = dcrconfig.ConfigManager().NodeFile.replace(config.ConfigManager().mountDirectory + '/', '')

    subprocess.call([config.ConfigManager().TransferScriptFile,
                     config.ConfigManager().jobServerPassword,
                     semanticGraph, intEdges, nodeDict,
                     config.ConfigManager().mountDirectory,
                     config.ConfigManager().knowledgeFilesBackup])
