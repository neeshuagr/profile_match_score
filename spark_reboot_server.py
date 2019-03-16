#!/usr/bin/python3.4
#   File transfer.
#   Runs shell script file to transfer files


import subprocess
import os
import config
import utility
import dcrconfig


if __name__ == "__main__":

    # Spark server reboot
    subprocess.call([config.ConfigManager().sparkRebootScript,
                    config.ConfigManager().webServerPassword,
                    config.ConfigManager().sparkJarFilePath,
                    config.ConfigManager().sparkContext,
                    config.ConfigManager().sparkNumCpuCores,
                    config.ConfigManager().sparkMemoryPerNode,
                    config.ConfigManager().sparkDriverMemory])
