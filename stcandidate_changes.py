import custom
import utility
import config
import datetime

utility.write_to_file(config.ConfigManager().LogFile,
                          'a', 'stcandidate_changes running' + ' ' + str(datetime.datetime.now()))
configdocs = custom.retrieve_data_from_DB(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB, config.ConfigManager().ConfigCollection)
custom.data_changes_from_DB(config.ConfigManager().STConnStr, config.ConfigManager().STCandidateChangesQueryId, config.ConfigManager().CandidateDetails, config.ConfigManager().ST, configdocs[0]['STCandidateChangesDate'])
