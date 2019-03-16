#!/usr/bin/python3.4
#   This libray will be used for requirement graph generations.



import dcrconfig
import config
import dcrgraphcompactor
from pymongo import MongoClient
from datetime import datetime


cl = MongoClient(dcrconfig.ConfigManager().Datadb)
db = cl[config.ConfigManager().DataCollectionDB]
datacol = db[config.ConfigManager().STReqCollection]
candidates = db[config.ConfigManager().STCandidateCollection]
print("%s %s %s %s" % (dcrconfig.ConfigManager().Datadb,
                       config.ConfigManager().DataCollectionDB,
                       config.ConfigManager().STReqCollection,
                       config.ConfigManager().STCandidateCollection))


# Defining these in the top to avoid repeated load.
# This is only used inside generated_req_candidete_file() function.
mapping_dict = dcrgraphcompactor.load_node_dict()
edge_int_dict = dcrgraphcompactor.get_normalized_dictionary()


def find_candidates(clist, requirementid):
    candidate_list = []
    for cand in clist:
        if ('requirementIDList' in cand.keys() and
           type(cand["requirementIDList"]) is list):

            for reqid in cand['requirementIDList']:
                if type(reqid) is not list:
                    break

                if int(reqid[1]) == requirementid:
                    candidate_list.append(cand["candidateid"])
                    break
    return candidate_list


# Create a file with requirement id and candidate integer graph inside.
def generate_req_candidate_file(file_name, req_candidate_list):
    # get the list from mongo db and save it.
    candidate_list = []
    for cand in candidates.find({"candidateid": {"$in": req_candidate_list}}):
        candidate_list.append({"id": cand["candidateid"],
                               "phrases": cand["nounPhrases"]})

    dcrgraphcompactor.generate_document_graphs_from_list(mapping_dict,
                                                         edge_int_dict,
                                                         candidate_list,
                                                         file_name)


def generate_req_candidate_file_edge_dict_from_file(file_name, req_candidate_list):
    # get the list from mongo db and save it.
    candidate_list = []
    for cand in candidates.find({"candidateid": {"$in": req_candidate_list}}):
        candidate_list.append({"id": cand["candidateid"],
                               "phrases": cand["nounPhrases"]})

    edge_int_dict_from_file = dcrgraphcompactor.get_normalized_dictionary_from_int_edges()
    dcrgraphcompactor.generate_document_graphs_from_list(mapping_dict,
                                                         edge_int_dict_from_file,
                                                         candidate_list,
                                                         file_name)


def generate_req_candidate_file_selected_req(req_list):

    candidate_list = list(candidates.find({}, {"candidateid": 1,
                                               "requirementIDList": 1}))

    # Remove the duplicates if it is coming from another list
    distinct_req_list = list(set(req_list))

    for req in distinct_req_list:
        # File name is the requirement Id with the path from config
        req_file_name = dcrconfig.ConfigManager().SmartTrackDirectory
        req_file_name += str(req)

        # Clear the file. This can be changed to
        # append if only the new candidates are  picked in the candidate list
        open(req_file_name, 'w').close()

        # Find candidates for the requirement and generate the
        # req candidate file.
        req_candidate_list = find_candidates(candidate_list, req)
        generate_req_candidate_file_edge_dict_from_file(req_file_name, req_candidate_list)


#   main function entry
if __name__ == "__main__":

    jcount = 0
    start = datetime.now()
    req_list = []

    candidate_list = list(candidates.find({}, {"candidateid": 1,
                                               "requirementIDList": 1}))

    # Create requirement list for which file need to be created.
    for doc in datacol.find({}, {"requirementid": 1}):
        req_list.append(doc['requirementid'])
        jcount += 1

    generate_req_candidate_file_selected_req(req_list)

    end = datetime.now()
    print(" %s %s total documents processed: %d"
          % (str(start), str(end), jcount))
