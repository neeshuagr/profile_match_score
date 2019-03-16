#!/usr/bin/python3.4
#   Symantic Graph compactor.
#   Reads a graph from a predefined file and optimizes by converting it into integer nodes


import networkx as nx
import dcrgraph
import dcrconfig
import sys
import csv
import utility
import datetime
import config

def generate_document_graphs(dict, edge_dict):
    #  Loop thru all phrase files and generate the integer graph
    phrase_file = open(dcrconfig.ConfigManager().DistinctPhraseFile, 'r')
    jdcount = 0
    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight

    for line in phrase_file:
        line = line.strip()

        if (line.startswith('--')):
            #  If the line starts with -- then it is job descriptin begenning
            #  So print a dot indicate the progress
            print('.', end='')
            sys.stdout.flush()
            doc = line.strip()

        if not (line.startswith('--') or len(line.strip()) < 1):
            graph = dcrgraph.create_graph_distant_neighbors(line, graph_weight)
            graph = dcrgraph.generate_document_integer_graph(dict,
                                                             graph,
                                                             doc,
                                                             edge_dict)
            jdcount += 1
            if jdcount % 10 == 0:
                print('%d' % jdcount)


def generate_document_graphs_from_dict_list(dict, edge_dict, list, directory):
    #  Loop thru all phrase files and generate the integer graph

    jdcount = 0
    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight

    for listitem in list:
        doc = '---' + str(listitem['doc_id']) + '---'
        filepath = directory + '/' + str(doc)
        graph = dcrgraph.create_graph_distant_neighbors(listitem
                                                        ['nounPhrases'],
                                                        graph_weight)
        graph = dcrgraph.generate_document_integer_graph(dict,
                                                         graph,
                                                         doc,
                                                         edge_dict, 'w',
                                                         filepath)
        jdcount += 1
        if jdcount % 10 == 0:
            print('%d' % jdcount)


def generate_document_graphs_from_list(dict,
                                       edge_dict,
                                       candidates,
                                       req_cand_file):

    #  Loop thru all phrase files and generate the integer graph
    jdcount = 0
    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight

    for candidate in candidates:
        line = candidate["phrases"]
        doc = '---' + str(candidate["id"]) + '---'

        print("writing %s" % req_cand_file)
        graph = dcrgraph.create_graph_distant_neighbors(line, graph_weight)
        graph = dcrgraph.generate_document_integer_graph(dict,
                                                         graph,
                                                         doc,
                                                         edge_dict,
                                                         'a',
                                                         req_cand_file)
        jdcount += 1
        if jdcount % 10 == 0:
            print('%d' % jdcount)


#   Function Name: generate_nodes
#   Description: From integer edges generate integer nodes
def generate_nodes():
    semantic_edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
    node_file = open(dcrconfig.ConfigManager().IntegerNodesFile, 'w')

    nodes = set()
    for line in semantic_edge_file:
        words = line.split()
        nodes.add(words[0])
        nodes.add(words[1])

    print('Saving integer nodes to file ...')
    for node in nodes:
        print("%s" % node, file=node_file)


def save_node_dict():
    import pickle
    print('Reading Semantic Graph...')
    graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    # create an integer mapping for each of the phrases
    # mapping_dict = dcrgraph.create_node_dictionary(graph.nodes())
    old_node_dict = load_node_dict()
    mapping_dict = dcrgraph.append_node_dictionary(graph.nodes(), old_node_dict)
    pickle.dump(mapping_dict, open(dcrconfig.ConfigManager().NodeFile, 'wb'))
    print('Saving nodes completed')


def load_node_dict():
    import pickle
    dict = pickle.load(open(dcrconfig.ConfigManager().NodeFile, 'rb'))
    return dict


def load_node_dict_file_path(filepath):
    import pickle
    dict = pickle.load(open(filepath, 'rb'))
    return dict


def get_normalized_dictionary():
    print('Reading Semantic Graph...')
    graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    # create an integer mapping for each of the phrases
    mapping_dict = load_node_dict()
    new_graph = nx.relabel_nodes(graph, mapping_dict)

    edge_int_dict = {}
    edge_count = 0

    # Loop thru the edges. Compare the nodes order the first node be greater
    # than the second one. This will help in compressing the graph
    for edge in new_graph.edges(data=True):
        edge1 = edge[0]
        edge2 = edge[1]
        if edge1 > edge2:
            edge1 = edge[1]
            edge2 = edge[0]
        edge_count += 1
        edge_int_dict[(edge1, edge2)] = edge_count

    return edge_int_dict


def get_normalized_dictionary_from_int_edges():
    edge_int_dict = {}
    edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
    for each_line in edge_file:
        edge_count = int((each_line.split(' '))[0])
        edge1 = int((each_line.split(' '))[1])
        edge2 = int((each_line.split(' '))[2])
        edge_int_dict[(edge1, edge2)] = edge_count

    return edge_int_dict


def create_edges_file():
    print('Reading Semantic Graph...')
    graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
    last_line = '0 '
    for each_line in edge_file:
        last_line = each_line
    edge_int_dict = {}
    edge_count_old_max = int((last_line.split(' '))[0])
    edge_file.close()

    # create an integer mapping for each of the phrases
    # mapping_dict = dcrgraph.create_node_dictionary(graph.nodes())
    old_node_dict = load_node_dict()
    mapping_dict = dcrgraph.append_node_dictionary(graph.nodes(), old_node_dict)
    # writer = csv.writer(open('/mnt/nlpdata/nodedict.csv', 'w'))
    # for key, value in mapping_dict.items():
    #     writer.writerow([key, value])

    new_graph = nx.relabel_nodes(graph, mapping_dict)

    print('Saving integer Semantic graph...')
    edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile.replace('int_edges', 'int_edges_new'), 'w')

    edge_int_dict = {}
    edge_count = 0

    # Loop thru the edges. Compare the nodes order the first node be greater
    # than the second one. This will help in compressing the graph
    for edge in new_graph.edges(data=True):
        edge1 = edge[0]
        edge2 = edge[1]
        if edge1 > edge2:
            edge1 = edge[1]
            edge2 = edge[0]
        edge_count += 1
        print('%d %d %d %d' % (edge_count, edge1, edge2, edge[2]['weight']),
              file=edge_file)
        edge_int_dict[(edge1, edge2)] = edge_count
    
    edge_file.close()
    merge_int_edges(edge_count_old_max)
    rewrite_strip_edges_file((dcrconfig.ConfigManager().IntegerEdegesFile).replace('int_edges', 'int_edges_temp'), dcrconfig.ConfigManager().IntegerEdegesFile)
    generate_nodes()


def merge_int_edges(edge_count_max):
    edge_int_dict = {}
    edge_file_temp = open(dcrconfig.ConfigManager().IntegerEdegesFile.replace('int_edges', 'int_edges_temp'), 'w')
    edge_file_new = open(dcrconfig.ConfigManager().IntegerEdegesFile.replace('int_edges', 'int_edges_new'), 'r')
    for each_line_new in edge_file_new:
        edge_count_new = int((each_line_new.split(' '))[0])
        edge1_new = int((each_line_new.split(' '))[1])
        edge2_new = int((each_line_new.split(' '))[2])
        edge_weight_new = int((each_line_new.split(' '))[3])
        edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
        repeat_flag = 0
        for each_line in edge_file:
            edge_count = int((each_line.split(' '))[0])
            edge1 = int((each_line.split(' '))[1])
            edge2 = int((each_line.split(' '))[2])
            edge_weight = int((each_line.split(' '))[3])
            if edge1_new == edge1 and edge2_new == edge2:
                repeat_flag = 1
                print('%d %d %d %d' % (edge_count, edge1, edge2, edge_weight_new), file=edge_file_temp)
                edge_int_dict[(edge1, edge2)] = edge_count
        edge_file.close()
        if repeat_flag == 0:
            edge_count_max += 1
            print('%d %d %d %d' % (edge_count_max, edge1_new, edge2_new, edge_weight_new), file=edge_file_temp)
            edge_int_dict[(edge1_new, edge2_new)] = edge_count_max
    edge_file_new.close()
    edge_file_temp.close()
    # edge_int_dict[(edge1, edge2)] = edge_count


def append_edges_file():
    print('Reading Semantic Graph...')
    graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    # create an integer mapping for each of the phrases
    # mapping_dict = dcrgraph.create_node_dictionary(graph.nodes())
    old_node_dict = load_node_dict()
    mapping_dict = dcrgraph.delta_node_dictionary(graph.nodes(), old_node_dict)
    # writer = csv.writer(open('/mnt/nlpdata/nodedict.csv', 'w'))
    # for key, value in mapping_dict.items():
    #     writer.writerow([key, value])

    new_graph = nx.relabel_nodes(graph, mapping_dict)
    print('Saving integer Semantic graph...')
    edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
    print(dcrconfig.ConfigManager().IntegerEdegesFile)
    last_line = '0 '
    for each_line in edge_file:
        last_line = each_line
    # last_line = each_line
    edge_int_dict = {}
    edge_count = int((last_line.split(' '))[0])
    print(last_line)
    print(edge_count)
    edge_file.close()
    edge_file_append = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'a')
    if new_graph.edges(data=True):
        print('%s' % (''), file=edge_file_append)
    # Loop thru the edges. Compare the nodes order the first node be greater
    # than the second one. This will help in compressing the graph
    for edge in new_graph.edges(data=True):
        if (isinstance(edge[0], int) and isinstance(edge[1], int)):
            edge1 = edge[0]
            edge2 = edge[1]
            if edge1 > edge2:
                edge1 = edge[1]
                edge2 = edge[0]
            edge_count += 1
            print(edge1, edge2)
            print(edge_count, edge1, edge2, edge[2]['weight'])
            print('%d %d %d %d' % (edge_count, edge1, edge2, edge[2]['weight']),
                  file=edge_file_append)
            edge_int_dict[(edge1, edge2)] = edge_count

    edge_file_append.close()
    rewrite_strip_edges_file(dcrconfig.ConfigManager().IntegerEdegesFile, (dcrconfig.ConfigManager().IntegerEdegesFile).replace('int_edges', 'int_edges_temp'))
    rewrite_strip_edges_file((dcrconfig.ConfigManager().IntegerEdegesFile).replace('int_edges', 'int_edges_temp'), dcrconfig.ConfigManager().IntegerEdegesFile)
    generate_nodes()


def rewrite_strip_edges_file(edge_file_name, edge_file_w_name):
    edge_file = open(edge_file_name, 'r')
    edge_file_w = open(edge_file_w_name, 'w')
    for each_line in edge_file:
        if each_line.rstrip():
            line_list = each_line.split(' ')
            print('%d %d %d %d' % (int(line_list[0]), int(line_list[1]), int(line_list[2]), int(line_list[3])),
                  file=edge_file_w)
    edge_file.close()
    edge_file_w.close()


def create_edges_file_with_dict():
    print('Reading Semantic Graph...')
    graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'r')
    edge_int_dict_old = {}
    last_line = '0 '
    for each_line in edge_file:
        last_line = each_line
        edge_count = int((each_line.split(' '))[0])
        edge1 = int((each_line.split(' '))[1])
        edge2 = int((each_line.split(' '))[2])
        edge_weight = int((each_line.split(' '))[3])
        edge_int_dict_old[(edge1, edge2)] = [edge_count, edge_weight]

    edge_count_old_max = int((last_line.split(' '))[0])
    edge_file.close()
    # create an integer mapping for each of the phrases
    # mapping_dict = dcrgraph.create_node_dictionary(graph.nodes())
    old_node_dict = load_node_dict()
    mapping_dict = dcrgraph.append_node_dictionary(graph.nodes(), old_node_dict)
    # writer = csv.writer(open('/mnt/nlpdata/nodedict.csv', 'w'))
    # for key, value in mapping_dict.items():
    #     writer.writerow([key, value])

    new_graph = nx.relabel_nodes(graph, mapping_dict)


    edge_int_dict = {}
    edge_count = 0

    # Loop thru the edges. Compare the nodes order the first node be greater
    # than the second one. This will help in compressing the graph
    for edge in new_graph.edges(data=True):
        edge1 = edge[0]
        edge2 = edge[1]
        if edge1 > edge2:
            edge1 = edge[1]
            edge2 = edge[0]
        edge_count += 1

        edge_int_dict[(edge1, edge2)] = [int(edge_count), int(edge[2]['weight'])]
    edge_file.close()
    merge_int_edges_with_dict(edge_count_old_max, edge_int_dict_old, edge_int_dict)
    generate_nodes()


def merge_int_edges_with_dict(edge_count_max, edge_int_dict_old, edge_int_dict):
    edge_file_new = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'w')
    edge_int_dict_new = edge_int_dict_old.copy()
    for key in edge_int_dict:
        if key in edge_int_dict_old:
            edge_int_dict_new[key] = [(edge_int_dict_old[key])[0], (edge_int_dict[key])[1]]
        else:
            edge_count_max += 1
            edge_int_dict_new[key] = [edge_count_max, (edge_int_dict[key])[1]]
    edge_int_dict.clear()
    edge_int_dict_old.clear()
    for key in sorted(edge_int_dict_new.keys(), key=lambda k: edge_int_dict_new[k][0]):
        print('%d %d %d %d' % ((edge_int_dict_new[key])[0], int(key[0]), int(key[1]), (edge_int_dict_new[key])[1]), file=edge_file_new)

    edge_file_new.close()
#   main function entry
if __name__ == "__main__":
    utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 8..! (dcrgraphcompactor.py) ' + str(datetime.datetime.now()))
    # ----------------------------------------------- Moved to append_edges_file() ------------------------------------------------#
    # print('Reading Semantic Graph...')
    # graph = nx.read_gexf(dcrconfig.ConfigManager().SemanticGraphFile)

    # # create an integer mapping for each of the phrases
    # # mapping_dict = dcrgraph.create_node_dictionary(graph.nodes())
    # old_node_dict = load_node_dict()
    # mapping_dict = dcrgraph.append_node_dictionary(graph.nodes(), old_node_dict)
    # # writer = csv.writer(open('/mnt/nlpdata/nodedict.csv', 'w'))
    # # for key, value in mapping_dict.items():
    # #     writer.writerow([key, value])

    # new_graph = nx.relabel_nodes(graph, mapping_dict)

    # print('Saving integer Semantic graph...')
    # edge_file = open(dcrconfig.ConfigManager().IntegerEdegesFile, 'w')

    # edge_int_dict = {}
    # edge_count = 0

    # # Loop thru the edges. Compare the nodes order the first node be greater
    # # than the second one. This will help in compressing the graph
    # for edge in new_graph.edges(data=True):
    #     edge1 = edge[0]
    #     edge2 = edge[1]
    #     if edge1 > edge2:
    #         edge1 = edge[1]
    #         edge2 = edge[0]
    #     edge_count += 1
    #     print('%d %d %d %d' % (edge_count, edge1, edge2, edge[2]['weight']),
    #           file=edge_file)
    #     edge_int_dict[(edge1, edge2)] = edge_count

    # edge_file.close()
    # generate_nodes()
    # ----------------------------------------------- Moved to append_edges_file() ------------------------------------------------#
    # append_edges_file()
    # create_edges_file()
    create_edges_file_with_dict()

#    print('Saving Integer Document Graphs...')
#    generate_document_graphs(mapping_dict, edge_int_dict)

# Write the integer nodes to file
# node_file = open('int_nodes.txt', 'w')
# for key in mapping_dict.values():
#    print(key, file=node_file)

# sys.getsizeof(graph)))
# nx.write_gexf(new_graph, 'integer_graph.gexf')
# for k, v in mapping_dict.items():
#    print('%s, %d' % (k, v))

# print('sub graph  %s' % (nx.info(graph)))
# node = graph.neighbors('net')
# print(node)
# node_list = graph.edges_iter('net', data=True)
# for e in node_list:
#   print('%s %s' % (e[0], e[1]))
#    print(e[2])
"""
node_list = list((u, v, d) for u, v, d in
                 graph.edges_iter(['net', 'sql server', 'healthcare'],
                 data=True) if (d['weight'] > 25))
sub_graph = nx.Graph(node_list)

print('sub graph  %s' % (nx.info(sub_graph)))

print('Saving ..')
dcrgraph.draw_graph(sub_graph, 'net_sqlserver_healthcare.pdf')


while True:
    lo = int(input('Input the Lower limit: '))
    hi = int(input('Input the upper limit: '))
    if hi < 10:
        break

    node_list = list((u, v, d) for u, v, d in
                     graph.edges_iter(data=True)
                     if (d['weight'] > lo) and (d['weight'] < hi))

    sub_graph = nx.Graph(node_list)
    print('sub graph  %s' % (nx.info(sub_graph)))
    # print(node_list)
    if nx.number_of_edges(sub_graph) < 500:
        dcrgraph.draw_graph(sub_graph, 'graf_' +
                            str(lo) + '_' + str(hi) + '.pdf')
"""


def generate_document_graphs_from_dict_list_savetodb(dict, edge_dict,
                                                     noun_phrases):
    #  Loop thru all phrase files and generate the integer graph

    jdcount = 0
    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight

    graph = dcrgraph.create_graph_distant_neighbors(noun_phrases,
                                                    graph_weight)
    graph = dcrgraph.generate_document_integer_graph_savetodb(dict,
                                                              graph,
                                                              edge_dict)
    jdcount += 1
    if jdcount % 10 == 0:
        # print('%d' % jdcount)
        test = ''
    return graph


def generate_document_signature_graph(dict, edge_dict, noun_phrases, neighborCount, diminition_percent):

    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight
    # noun_phrases = "|human|asst|level|performs variety|general personnel clerical tasks|areas|employee|education training|employment|compensation|equal employment opportunity.|personnel|compiles sensitive|confidential personnel.|accordance|information.|provides information|personnel.|mba hr."
    # graph = dcrgraph.create_document_graph_distant_neighbors(noun_phrases, neighborCount, diminition_percent, graph_weight)
    graph = dcrgraph.create_graph_distant_neighbors_with_generator(noun_phrases, graph_weight)
    graph = dcrgraph.graph_to_signature_graph(dict, graph, edge_dict)
    return graph
