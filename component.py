from PyPDF2 import PdfFileReader
import docx
from xlrd import open_workbook
from subprocess import Popen, PIPE
import csv
import xml.etree.ElementTree as ET
from collections import Counter
import sys
import os
import string
import re
from docx import Document
import docx2txt
from pymongo import MongoClient
import config
import dcrconfig
import utility

import tika
tika.initVM()
from tika import parser

cl = MongoClient(dcrconfig.ConfigManager().Datadb)
# db = cl[config.ConfigManager().resumesDetectDb]
db = cl[config.ConfigManager().DataCollectionDB]
collection = db[config.ConfigManager().STSupplierCollection]
supplierExceptions = db[config.ConfigManager().supplierNameExceptions]


stcl = MongoClient(dcrconfig.ConfigManager().Datadb)
stdb = stcl[config.ConfigManager().resumesDetectDb]
extensionExceptions = stdb[config.ConfigManager().extensionExceptions]

# Image detection test with the actual resumes.
imgtypes = ['JPG', 'GIF', 'GIF', 'PNG', 'TIF', 'BMP', 'psd']
imgsigs = ['JFIF', 'GIF87a', 'GIF89a', 'RGB', 'EPS', 'DIB', '8BPS']
imgsigoffs = [6, 0, 0, 0, 0, 0, 0, 0]
imgmarker = []


def image_detect(filepath):
    imgmarker = []
    flagList = []
    flagDetails = []
    filename = os.path.abspath(filepath)
    if not os.path.isfile(filename):
        print('Error: No such file ', filename)
        sys.exit(2)
    try:
        infile = open(filename, 'rb')
    except:
        print('Could not open file to read !', filename)
        sys.exit(3)
    if infile is None:
        print('Error opening file ', filename)
        sys.exit(3)
    c = infile.read(1)

    lastmatch = ""
    while c != ''.encode():

        for x in range(0, len(imgsigs)):
            sig = imgsigs[x]
            if c == sig[0].encode():
                lentoread = len(sig) - 1
                current_pos = int(infile.tell())
                chunk = c + infile.read(lentoread)

                if chunk == sig.encode():
                    fpos = int(infile.tell())
                    sigpos = fpos - len(sig)
                    imgpos = sigpos - imgsigoffs[x]
                    imgmarker.append((imgpos, imgtypes[x]))
                    lastmatch = imgtypes[x]
                else:
                    currpos = int(infile.tell())
                    # prevpos = currpos-lentoread
                    prevpos = current_pos
                    infile.seek(prevpos)
        c = infile.read(1)
    posn = int(infile.tell())
    imgmarker.append((posn, lastmatch))
    if imgmarker[0][1] != '':
        print("Image detection: Yes")
        flagDetails.append("Image detected")
        flagList.append(1)
    else:
        print("Image detection: No")
        flagList.append(0)
    return flagList, flagDetails


def read_docx_text(filepath, supplierID):
    imageInfo = image_detect(filepath)
    flagList = []
    flagDetails = {}
    data_text = docx2txt.process(filepath)
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails


# def detect_supplier_info(data_text, supplierName):
#     flagDetails = []
#     supplierFlag = False
#     supplier_exception_collection = supplierExceptions.find()
#     exceptionList = utility.fill_list_from_db_object(supplier_exception_collection, 'supplierName')
#     exceptionList = utility.convert_list_items_to_lower_case(exceptionList)
#     rm_dump_spaces = " ".join(data_text.split())
#     rm_space = rm_dump_spaces
#     supplier = supplierName
#     rm_dump_spaces = " ".join(supplier.split())
#     rm_space_supplier = rm_dump_spaces

#     if rm_space_supplier.lower() not in exceptionList:
#         text_found = utility.find_word(rm_space.lower(), rm_space_supplier.lower())
#         if text_found:
#             supplierFlag = True
#             flagDetails.append(rm_space_supplier)
#     if supplierFlag is False:
#         extensions = extensionExceptions.find({}, {"_id": 0})
#         for extension in extensions:
#             extension = extension.get("Name")
#             supplierExtension = " ".join(extension.split())
#             supplierName = rm_space_supplier.lower()
#             supplierExtension = supplierExtension.lower()
#             res = utility.find_word(supplierName, supplierExtension)
#             if res:
#                 supplierName = supplierName.replace(str(res[0]), '')
#                 text_found = utility.find_word(rm_space.lower(), supplierName)
#                 print(text_found)
#                 if text_found:
#                     supplierFlag = True
#                     flagDetails.append(supplierName)

#     return supplierFlag, flagDetails


def detect_supplier_info(data_text, supplierID):
    flagDetails = []
    supplierFlag = False
    supplier_exception_collection = supplierExceptions.find()
    exceptionList = utility.fill_list_from_db_object(supplier_exception_collection, 'supplierName')
    exceptionList = utility.convert_list_items_to_lower_case(exceptionList)
    rm_dump_spaces = " ".join(data_text.split())
    # rm_space = rm_dump_spaces.replace(" ", "")
    rm_space = rm_dump_spaces
    dbcollection = collection.find({"supplierID": supplierID})
    for dbcollectionname in dbcollection:
        supplier = dbcollectionname.get("Name")
        rm_dump_spaces = " ".join(supplier.split())
        # rm_space_supplier = rm_dump_spaces.replace(" ", "")
        rm_space_supplier = rm_dump_spaces
        # if rm_space_supplier.lower() in rm_space.lower() and rm_space_supplier.lower() not in exceptionList:

        if rm_space_supplier.lower() not in exceptionList:
            text_found = utility.find_word(rm_space.lower(), rm_space_supplier.lower())
            if text_found:
                supplierFlag = True
                flagDetails.append(rm_space_supplier)
        if supplierFlag is False:
            extensions = extensionExceptions.find({}, {"_id": 0})
            for extension in extensions:
                extension = extension.get("Name")
                supplierExtension = " ".join(extension.split())
                supplierName = rm_space_supplier.lower()
                supplierExtension = supplierExtension.lower()
                res = utility.find_word(supplierName, supplierExtension)
                if res:
                    supplierName = supplierName.replace(str(res[0]), '')
                    if supplierName.lower() not in exceptionList:
                        text_found = utility.find_word(rm_space.lower(), supplierName)
                        if text_found:
                            supplierFlag = True
                            flagDetails.append(supplierName)
    return supplierFlag, flagDetails


# def detect_supplier_info(data_text):
#     flagDetails = []
#     supplierFlag = False
#     supplier_exception_collection = supplierExceptions.find()
#     exceptionList = utility.fill_list_from_db_object(supplier_exception_collection, 'supplierName')
#     exceptionList = utility.convert_list_items_to_lower_case(exceptionList)
#     rm_dump_spaces = " ".join(data_text.split())
#     # rm_space = rm_dump_spaces.replace(" ", "")
#     rm_space = rm_dump_spaces
#     dbcollection = collection.find()
#     for dbcollectionname in dbcollection:
#         supplier = dbcollectionname.get("Name")
#         rm_dump_spaces = " ".join(supplier.split())
#         # rm_space_supplier = rm_dump_spaces.replace(" ", "")
#         rm_space_supplier = rm_dump_spaces
#         # if rm_space_supplier.lower() in rm_space.lower() and rm_space_supplier.lower() not in exceptionList:

#         if rm_space_supplier.lower() not in exceptionList:
#             text_found = utility.find_word(rm_space.lower(), rm_space_supplier.lower())
#             if text_found:
#                 supplierFlag = True
#                 flagDetails.append(rm_space_supplier)
#             # flagList.append(1)
#             # return flagList
#     # flagList.append(0)
#     return supplierFlag, flagDetails


def read_doc_text_catdoc(filepath, supplierID):
    imageInfo = image_detect(filepath)
    flagList = []
    flagDetails = {}
    data_text = ''
    fileopen = os.popen('catdoc -w "%s"' % filepath)
    data_text = fileopen.read()
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails


def read_pdf_text(filepath, supplierID):
    imageInfo = image_detect(filepath)
    data_text = ''
    flagList = []
    flagDetails = {}
    # pdf_file_object = open(filepath, 'rb')
    # pdf_file = PdfFileReader(pdf_file_object)
    # for page in pdf_file.pages:
    #     data_text += page.extractText()
    # make the text in oneline remove new line charecter
    parsed = parser.from_file(filepath)
    data_text += parsed["content"]
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails


def read_text_text(filepath, supplierID):
    imageInfo = image_detect(filepath)
    flagList = []
    flagDetails = {}
    data_text = ''
    text_file = open(filepath, 'r')
    data_text = text_file.read()
    text_file.close()
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails


def read_odt_text(filepath, supplierID):
    imageInfo = image_detect(filepath)
    flagList = []
    flagDetails = {}
    data_text = ''
    popen_param = ['odt2txt', filepath]
    popen_output = Popen(popen_param, stdout=PIPE)
    stdout, stderr = popen_output.communicate()
    data_text += stdout.decode('ascii', 'ignore')
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails


def read_rtf_text_catdoc(filepath):
    image = image_detect(filepath)
    data_text = ''
    flagList = []
    fileopen = os.popen('catdoc -w "%s"' % filepath)
    data_text = fileopen.read()
    rm_dump_spaces = data_text.rstrip('\n').split(',')
    data = [i.replace('\n', ' ') for i in rm_dump_spaces]
    text_oneline = str(data)
    email_url_phone = utility.email_phone_url(text_oneline)
    supplier = detect_supplier_info(data_text)
    flagList.append(image)
    flagList.append(email_url_phone)
    flagList.append(supplier)
    return flagList


def read_rtf_text(filepath):
    base_file, ext = os.path.splitext(filepath)
    if ext == ".rtf":
        text = os.rename(filepath, base_file + ".doc")
        read_rtf_text_catdoc(base_file + ".doc")
        detect_supplier_info(base_file + ".doc")


def read_file_content(filepath, supplierID):
    imageInfo = image_detect(filepath)
    flagList = []
    flagDetails = {}
    data_text = ''
    parsed = parser.from_file(filepath)
    data_text += parsed["content"]
    rm_newline_char = data_text.rstrip('\n').split(',')
    rp_newline_empty = [i.replace('\n', ' ') for i in rm_newline_char]
    rp_newline_empty = [i.replace('\t', ' ') for i in rp_newline_empty]
    text_oneline = str(rp_newline_empty)
    email_url_phone = utility.email_phone_url(text_oneline)
    email_url_phoneFlags = email_url_phone[0]
    flagDetails = email_url_phone[1]
    supplierInfo = detect_supplier_info(data_text, supplierID)
    supplierBoolean = supplierInfo[0]
    imageBoolean = imageInfo[0]
    if imageBoolean[0]:
        flagList.append(imageBoolean)
        imageValue = imageInfo[1]
        flagDetails['image'] = imageValue
    else:
        flagList.append(imageBoolean)
    flagList.append(email_url_phoneFlags)
    suppliers = []
    if supplierBoolean:
        supplierVals = supplierInfo[1]
        flagDetails['suppliers'] = supplierVals
        suppliers.append(1)
        flagList.append(suppliers)
    else:
        suppliers.append(0)
        flagList.append(suppliers)
    return flagList, flagDetails
