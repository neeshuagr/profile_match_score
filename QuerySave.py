import utility
import config
import dictionaries
import custom
import dbmanager

connection = dbmanager.mongoDB_connection_external_host(
    int(config.ConfigManager().MongoDBPort))


filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'XchangeRequirementQuery.txt'
read_type = 'r'

UpdateTemplateSet = utility.clean_dict()
UpdateTemplateWhere = utility.clean_dict()

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 1
dictionaries.UpdateTemplateSet['query_id'] = 1
dictionaries.UpdateTemplateSet['query_name'] = 'Xchange job details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'XchangeCandidateQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 2
dictionaries.UpdateTemplateSet['query_id'] = 2
dictionaries.UpdateTemplateSet['query_name'] = 'Xchange candidate details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STRequirementQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 3
dictionaries.UpdateTemplateSet['query_id'] = 3
dictionaries.UpdateTemplateSet['query_name'] = 'ST job details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 4
dictionaries.UpdateTemplateSet['query_id'] = 4
dictionaries.UpdateTemplateSet['query_name'] = 'ST candidate details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateSubmissionsQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 5
dictionaries.UpdateTemplateSet['query_id'] = 5
dictionaries.UpdateTemplateSet[
    'query_name'] = 'ST candidate submission details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateResumesQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 6
dictionaries.UpdateTemplateSet['query_id'] = 6
dictionaries.UpdateTemplateSet['query_name'] = 'ST candidate resume details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STReqDocQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 7
dictionaries.UpdateTemplateSet['query_id'] = 7
dictionaries.UpdateTemplateSet['query_name'] = 'ST req doc details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateStatusQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 8
dictionaries.UpdateTemplateSet['query_id'] = 8
dictionaries.UpdateTemplateSet['query_name'] = 'ST candidate status details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'XchangeCandidateResumesQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 9
dictionaries.UpdateTemplateSet['query_id'] = 9
dictionaries.UpdateTemplateSet['query_name'] = 'Xchange candidate resume details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STRequirementChangesQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 10
dictionaries.UpdateTemplateSet['query_id'] = 10
dictionaries.UpdateTemplateSet['query_name'] = 'ST requirement change details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)


filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateChangesQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 11
dictionaries.UpdateTemplateSet['query_id'] = 11
dictionaries.UpdateTemplateSet['query_name'] = 'ST candidate change details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'xCHANGECandidateChangesQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 12
dictionaries.UpdateTemplateSet['query_id'] = 12
dictionaries.UpdateTemplateSet['query_name'] = 'Xchange candidate change details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STSupplierDetailQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 13
dictionaries.UpdateTemplateSet['query_id'] = 13
dictionaries.UpdateTemplateSet['query_name'] = 'ST supplier details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'ClientQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 14
dictionaries.UpdateTemplateSet['query_id'] = 14
dictionaries.UpdateTemplateSet['query_name'] = 'ST Client details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)


filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'MSPQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 15
dictionaries.UpdateTemplateSet['query_id'] = 15
dictionaries.UpdateTemplateSet['query_name'] = 'ST MSP details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'laborCatagoryQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 16
dictionaries.UpdateTemplateSet['query_id'] = 16
dictionaries.UpdateTemplateSet['query_name'] = 'ST labor Catagory details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'typeOfServiceQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 17
dictionaries.UpdateTemplateSet['query_id'] = 17
dictionaries.UpdateTemplateSet['query_name'] = 'ST type of Service details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'currencyQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 18
dictionaries.UpdateTemplateSet['query_id'] = 18
dictionaries.UpdateTemplateSet['query_name'] = 'currency details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'industryQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 19
dictionaries.UpdateTemplateSet['query_id'] = 19
dictionaries.UpdateTemplateSet['query_name'] = 'industry details'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STReqCandidateRates.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
print(file_data)
dictionaries.UpdateTemplateWhere['query_id'] = 20
dictionaries.UpdateTemplateSet['query_id'] = 20
dictionaries.UpdateTemplateSet['query_name'] = 'st req candidate rates info'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STReqRates.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
print(file_data)
dictionaries.UpdateTemplateWhere['query_id'] = 21
dictionaries.UpdateTemplateSet['query_id'] = 21
dictionaries.UpdateTemplateSet['query_name'] = 'st req rates info'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STPromtCloudDataQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 22
dictionaries.UpdateTemplateSet['query_id'] = 22
dictionaries.UpdateTemplateSet[
    'query_name'] = 'ST Promtcloud Data info'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)


filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'STCandidateCurrencyQuery.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 23
dictionaries.UpdateTemplateSet['query_id'] = 23
dictionaries.UpdateTemplateSet[
    'query_name'] = 'ST Candidate Currency Update'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)

filepath = config.ConfigManager().QueryFilesDirectory + \
    '/' + 'GeoDataFetch.txt'
read_type = 'r'

file_data = utility.read_from_file(filepath, read_type)
dictionaries.UpdateTemplateWhere['query_id'] = 24
dictionaries.UpdateTemplateSet['query_id'] = 24
dictionaries.UpdateTemplateSet[
    'query_name'] = 'Geo Data Fetch'
dictionaries.UpdateTemplateSet['query'] = file_data
dictionaries.UpdateTemplateSet['var_list'] = ''
dictionaries.DBSet['$set'] = dictionaries.UpdateTemplateSet
custom.update_data_to_Db_con(int(config.ConfigManager().MongoDBPort), config.ConfigManager().DataCollectionDB,
                             config.ConfigManager().QueryCollection, dictionaries.UpdateTemplateWhere, dictionaries.DBSet['$set'], connection)
print(" connection =>", connection)
