#!/usr/bin/python3.4
#   Generates integer graphs for documents from a phrase file
#   Reads a graph from a predefined file and optimizes
#   by converting it into integer nodes


import networkx as nx
import dcrgraphcompactor
import dcrconfig
import utility
import datetime

#   main function entry
if __name__ == "__main__":
    utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 10..! (dcrdocumentintgraphgenerator.py) ' + str(datetime.datetime.now()))
    mapping_dict = dcrgraphcompactor.load_node_dict()
    # edge_int_dict = dcrgraphcompactor.get_normalized_dictionary()
    edge_int_dict = dcrgraphcompactor.get_normalized_dictionary_from_int_edges()
    print('Saving Integer Document Graphs...')
    dcrgraphcompactor.generate_document_graphs(mapping_dict, edge_int_dict)
    print("Successfully Completed.!")
