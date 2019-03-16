#!/usr/bin/python3.4
#   File transfer.
#   Runs shell script file to transfer files


import subprocess
import os
import config
import utility
import dcrconfig


if __name__ == "__main__":
    fileTransferDestination = config.ConfigManager().webServerIp + ':' + config.ConfigManager().mountDirectory
    semanticGraph = dcrconfig.ConfigManager().SemanticGraphFile.replace(config.ConfigManager().mountDirectory + '/', '')
    intEdges = dcrconfig.ConfigManager().IntegerEdegesFile.replace(config.ConfigManager().mountDirectory + '/', '')
    nodeDict = dcrconfig.ConfigManager().NodeFile.replace(config.ConfigManager().mountDirectory + '/', '')

    # Transferring knowledge files before spark server reboot
    subprocess.call([config.ConfigManager().knowledgeFilesTransferScript,
                    config.ConfigManager().webServerPassword,
                    semanticGraph,
                    intEdges,
                    nodeDict,
                    fileTransferDestination,
                    config.ConfigManager().knowledgeFilesBackup])