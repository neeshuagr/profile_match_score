import pymongo
import pyodbc
import dictionaries
import config


def mongoDB_connection(port_number):
    connection = pymongo.MongoClient(config.ConfigManager().mongoDBHost, port_number)
    return connection


def db_create(connection, db_name):
    db_name = db_name
    data_base = connection[db_name]


def db_connection(connection, db_name):
    data_base = getattr(connection, db_name)
    return data_base


def create_collection(data_base, collection_name):
    collection = data_base[collection_name]
    return collection


def retrieve_collection(data_base, collection_name):
    collection = getattr(data_base, collection_name)
    return collection


def crud_create(data_base, collection_name, document_data):
    collection = retrieve_collection(data_base, collection_name)
    collection.insert(document_data)


def crud_read(data_base, collection):
    documents = collection.find()
    return documents


def crud_update(collection, updatewhere, updateset):
    collection.update(updatewhere, updateset, upsert=True)


def crud_update_noupsert(collection, updatewhere, updateset):
    collection.update(updatewhere, updateset, upsert=False, multi=True)


def crud_update_noupsertsingle(collection, updatewhere, updateset):
    collection.update(updatewhere, updateset, upsert=False, multi=False)


def crud_read_one(data_base, collection, condition):
    document = collection.find(condition)
    return document


def crud_read_one_notimeout(data_base, collection, condition):
    document = collection.find(condition, no_cursor_timeout=True)
    return document


def crud_read_one_projection(data_base, collection, condition, projection):
    document = collection.find(condition, projection)
    return document


def crud_read_one_notimeout_projection(data_base, collection, condition, projection):
    document = collection.find(condition, projection, no_cursor_timeout=True)
    return document


def cursor_odbc_connection(con_string):
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    return cursor


def cursor_execute(cursor, query):
    cursor_exec = cursor.execute(query)
    dbdata = cursor.fetchall()
    dictionaries.ODBCdata['cursor_exec'] = cursor_exec
    dictionaries.ODBCdata['dbdata'] = dbdata
    return dictionaries.ODBCdata


def mongo_db_connection_with_host(port_number, host):
    connection = pymongo.MongoClient(host, port_number)
    return connection


def mongoDB_connection_external_host(port_number):
    connection = pymongo.MongoClient(config.ConfigManager().ExternalHost, port_number)
    return connection