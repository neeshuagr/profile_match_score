import tarfile
import datetime
import os
import config
import datetime
import utility

currentDate = str(datetime.datetime.now())
currentDate = currentDate.split(" ")
today = currentDate[0]

utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'promptcloud folder compression and upload' + ' ' + str(datetime.datetime.now()))
fileName = config.ConfigManager().PCRatesCompFolder + "_" + str(today)+".tar.gz"
#tar = tarfile.open(fileName, "w:gz")
#for name in ["/mnt/nlpdata/PCCompData"]:
#    tar.add(name)
#tar.close()

os.system("tar -zcvf " + fileName + " " + config.ConfigManager().PCRatesCompFolder)

# move to server
os.system("aws s3 cp " + fileName + " " + config.ConfigManager().PromptCloudS3BucketBackup + "/" + " --profile default")
# remove inside folder files in local
os.system("rm " + fileName)
