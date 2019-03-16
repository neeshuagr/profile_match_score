#!/usr/bin/python3.4
#   This is an application to create pdf graphsj.
#   Reads integer document graphs and prints them into a pdf

import networkx as nx
import dcrgraph
import dcrconfig
import sys
import matplotlib.pyplot as plt


def generate_graph_image(edges, doc_id):
    graph = nx.Graph()
    path = '/home/neeshu/Data/nlpdata/graphs/'
    graph.add_edges_from((v[1], v[2], {"weight": v[3]}) for v in edges)
    dcrgraph.draw_graph(graph, path + doc_id + '.pdf', (60, 40))


def generate_document_graph_images():
    #  Loop thru all phrase files and generate the integer graph
    phrase_file = open(dcrconfig.ConfigManager().DocumentsEdgesIntegerFile,
                       'r')
    jdcount = 0
    doc = ''

    node_collection = []
    for line in phrase_file:
        line = line.strip()

        if (line.startswith('--')):
            #  If the line starts with -- then it is job descriptin begenning
            #  So print a dot indicate the progress
            print('.', end='')
            sys.stdout.flush()
            if node_collection:
                generate_graph_image(node_collection, doc)
                node_collection = []
                jdcount += 1

            doc = line.strip()
        if not (line.startswith('--') or len(line.strip()) < 1):
            node_collection.append(line.split(' '))

        if jdcount > 50:
            break
        elif jdcount % 20 == 0:
            plt.close('all')


#   main function entry
if __name__ == "__main__":
    generate_document_graph_images()
