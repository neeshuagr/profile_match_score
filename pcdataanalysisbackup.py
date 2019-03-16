import tarfile
import datetime
import os
import config
import utility

utility.write_to_file(config.ConfigManager().PromptcloudLogFile, 'a', 'promptcloud analysis file backup running' + ' ' + str(datetime.datetime.now()))
current_date = (str(datetime.datetime.now()).split(" "))[0]
analysis_file = config.ConfigManager().PCDataAnalysisResultsFile.replace('.ods', current_date + '.ods')

# move to s3 bucket
os.system("aws s3 cp " + analysis_file + " s3://dcr-analytics-backups/pcdataanalysisfilebackup/ --profile default")
# remove inside folder files in local
os.system("echo 'b' | sudo -S rm " + analysis_file)
