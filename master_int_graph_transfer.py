#!/usr/bin/python3.4
#   File transfer.
#   Runs shell script file to transfer files


import subprocess
import os
import config
import utility
import dcrconfig


if __name__ == "__main__":
        subprocess.call([config.ConfigManager().transferMasterIntGraphFile,
                        config.ConfigManager().webServerIp,
                        config.ConfigManager().masterDocumentIntegerFile,
                        config.ConfigManager().webServerPassword])
