#   DCR Graph Generation Utility Module

import networkx as nx
import matplotlib.pyplot as plt
from random import random
import math
import dcrconfig
import os.path
import operator
from itertools import combinations
from itertools import product

#   Create graph with adjacent node with default weight 1.
#   This will be the base graph on top, the weighted graph will be
#   merged
def create_graph(phrase_string, edge_weight=1):
    ph = phrase_string.split('|')
    phrases = [s for s in ph if len(s) > 2]
    graph = nx.Graph()
    graph.add_nodes_from(phrases)
    # Add all edges
    i = 0
    while i < (len(phrases) - 1):
        graph.add_edge(phrases[i], phrases[i + 1], weight=edge_weight)
        i += 1

    return graph


def create_graph_distant_neighbors(phrase_string, edge_weight=1):
    phrase_sentences = phrase_string.split('.')
    base_graph = create_graph(phrase_string.replace('.', ''))
    neighbor_sensitive_graph = nx.Graph()
    diminition_percent = dcrconfig.ConfigManager().DiminitionPercentage

    for sent in phrase_sentences:
        ph = sent.split('|')
        phrases = [s for s in ph if len(s) > 2]
        neighbor_sensitive_graph.add_nodes_from(phrases)

        # Add all edges
        phrase_len = len(phrases)
        for i in range(phrase_len - 1):
            edge_weight = dcrconfig.ConfigManager().GraphEdgeWeight
            for j in range(i + 1, phrase_len):
                neighbor_sensitive_graph.add_edge(phrases[i],
                                                  phrases[j],
                                                  weight=edge_weight)

                #   Reduce the graph weight by the predefined percentage
                edge_weight = math.floor(edge_weight
                                         * diminition_percent / 100)
                #   If the edge_weight diminishes to less than 1,
                #   then you don't need to proceed.
                if edge_weight < 1:
                    break

    union_graph(base_graph, neighbor_sensitive_graph)
    return base_graph


def st_create_graph_distant_neighbors(phrase_string, edge_weight=1):
    phrase_sentences = phrase_string.split('.')
    base_graph = create_graph(phrase_string.replace('.', ''))
    neighbor_sensitive_graph = nx.Graph()
    diminition_percent = dcrconfig.ConfigManager().STDiminitionPercentage

    for sent in phrase_sentences:
        ph = sent.split('|')
        phrases = [s for s in ph if len(s) > 2]
        neighbor_sensitive_graph.add_nodes_from(phrases)

        # Add all edges
        phrase_len = len(phrases)
        for i in range(phrase_len - 1):
            edge_weight = dcrconfig.ConfigManager().STGraphEdgeWeight
            for j in range(i + 1, phrase_len):
                neighbor_sensitive_graph.add_edge(phrases[i],
                                                  phrases[j],
                                                  weight=edge_weight)

                #   Reduce the graph weight by the predefined percentage
                edge_weight = math.floor(edge_weight
                                         * diminition_percent / 100)
                #   If the edge_weight diminishes to less than 1,
                #   then you don't need to proceed.
                if edge_weight < 1:
                    break

    union_graph(base_graph, neighbor_sensitive_graph)
    return base_graph


#   Union two weighted graphs.
#   If there are two edges similar add their weights
def union_graph(destination, source, edge_weight=1):
    for v1, v2, w in source.edges_iter(data=True):
        if destination.has_edge(v1, v2):
            destination[v1][v2]['weight'] += w['weight']
        else:
            destination.add_edge(v1, v2, weight=w['weight'])
    return destination


def print_graph(graph):
    print('\n\nGraph Details : %s' % nx.info(graph))
    for v1, v2, w in graph.edges_iter(data=True):
        print('%s %s %s' % (v1, v2, w))


def draw_graph(graph, filename='graph.pdf', figuresize=(240, 180)):
    print('generating : %s' % filename)
    plt.figure(figsize=figuresize)
    # nx.draw(graph, with_labels=True)
    pos = nx.shell_layout(graph)
    edge_labels = dict([((u, v, ), d['weight'])
                        for u, v, d in graph.edges(data=True)])
    nx.draw_networkx_nodes(graph, pos, node_size=1000, node_color="white")

    colors = [((0.5058823529411764 + i / 100,
                0.6941176470588235 + i / 100, random()))
              for i in range(10)]
    # edges
    nx.draw_networkx_nodes(graph, pos, node_size=1000, node_color=colors)
    nx.draw_networkx_edges(graph, pos,
                           width=3, alpha=0.5, edge_color='#6699FF')
    nx.draw_networkx_edge_labels(graph,
                                 pos,
                                 edge_labels=edge_labels, label_pos=0.3)
    nx.draw_networkx_labels(graph, pos, font_size=10, font_family='sans-serif')

    plt.savefig(filename)
    # plt.show()
# graph_collection.append(graph)


def create_node_dictionary(graph_nodes):
    graph_nodes.sort()
    node_dict = dict(zip(graph_nodes, range(len(graph_nodes))))
    return node_dict


def append_node_dictionary(graph_nodes, old_node_dict):
    # empty list for new nodes
    new_node_list = []
    graph_nodes.sort()
    node_dict = dict(zip(graph_nodes, range(len(graph_nodes))))
    # innerjoin_dict = dict([[k, node_dict.get(k, 0)] for k in old_node_dict])
    # new nodes  in the form of dictionary
    new_nodes_dict = {key: node_dict[key] for key in
                      node_dict if key not in old_node_dict}
    # for key in new_nodes_dict.keys():
    #     new_node_list.append(key)
    # keys of new dictionary made into a list
    [new_node_list.append(key) for key in new_nodes_dict.keys()]
    new_node_list.sort()
    # new node dictionary created with node ids startign from 0
    new_node_list_dict = dict(zip(new_node_list, range(len(new_node_list))))
    # max node id of old dictionary
    max_node_id = old_node_dict.get(max(old_node_dict, key=old_node_dict.get))
    # add 1 since new_node_list_dict index starts from 0
    max_node_id = max_node_id + 1
    # add max_node_id to each value of the dictionary
    new_node_list_dict.update((x, (y + max_node_id)) for x, y in
                              new_node_list_dict.items())
    # merging old and new dictionaries
    merged_dict = dict(list(new_node_list_dict.items())+list
                       (old_node_dict.items()))
    return merged_dict

def delta_node_dictionary(graph_nodes, old_node_dict):
    # empty list for new nodes
    new_node_list = []
    graph_nodes.sort()
    node_dict = dict(zip(graph_nodes, range(len(graph_nodes))))
    # innerjoin_dict = dict([[k, node_dict.get(k, 0)] for k in old_node_dict])
    # new nodes  in the form of dictionary
    new_nodes_dict = {key: node_dict[key] for key in
                      node_dict if key not in old_node_dict}
    # for key in new_nodes_dict.keys():
    #     new_node_list.append(key)
    # keys of new dictionary made into a list
    [new_node_list.append(key) for key in new_nodes_dict.keys()]
    new_node_list.sort()
    # new node dictionary created with node ids startign from 0
    new_node_list_dict = dict(zip(new_node_list, range(len(new_node_list))))
    # max node id of old dictionary
    max_node_id = old_node_dict.get(max(old_node_dict, key=old_node_dict.get))
    # add 1 since new_node_list_dict index starts from 0
    max_node_id = max_node_id + 1
    # add max_node_id to each value of the dictionary
    new_node_list_dict.update((x, (y + max_node_id)) for x, y in
                              new_node_list_dict.items())
    
    merged_dict = dict(list(new_node_list_dict.items()))
    return merged_dict

def remove_missing_nodes(integer_dict, document_graph):
    nodes = document_graph.nodes()
    print(document_graph)
    for n in nodes:
        if n not in integer_dict:
            print('^', end='')
            document_graph.remove_node(n)
    return document_graph


def relabel_nodes(document_graph, integer_dict):
    return nx.relabel_nodes(document_graph, integer_dict)


def generate_document_integer_graph(integer_dict,
                                    document_graph,
                                    document,
                                    edge_dict,
                                    file_operation='a',
                                    edge_file_path=''):
    #   Generate compacted/Integer subgraph based on the integer dictionary.
    #   If the file_operation = a append to the configuration document graph

    #   Step1: Remove the nodes that is not present in the dictionary
    document_graph = remove_missing_nodes(integer_dict, document_graph)

    #   Step2: Integerize nodes
    graph = relabel_nodes(document_graph, integer_dict)

    #   Step3: Save int the file
    #   Check if the file path is not empty
    if edge_file_path == '':
        edge_file_path = dcrconfig.ConfigManager().DocumentsEdgesIntegerFile

    edge_file = open(edge_file_path, file_operation)
    print(document, file=edge_file)
    for edge in graph.edges(data=True):
        edge1 = edge[0]
        edge2 = edge[1]
        if edge1 > edge2:
            edge1 = edge[1]
            edge2 = edge[0]
        key = edge1, edge2
        if key in edge_dict:
            print('%d %d %d %d' % (edge_dict[key],
                                   edge1, edge2, edge[2]['weight']),
                  file=edge_file)
    edge_file.close()
    return graph


def generate_document_integer_nodes(integer_dict, document_graph):
    #   Generates Intiger nodes for the given document graph and dictionary
    #   Step1: Remove the nodes that is not present in the dictionary
    document_graph = remove_missing_nodes(integer_dict, document_graph)

    #   Step2: Integerize nodes
    graph = relabel_nodes(document_graph, integer_dict)
    return graph.nodes()


def load_graph(file_name):
    # Load semantic graph if it is already present in the system
    graph = nx.Graph()
    if os.path.isfile(file_name):
        print("File found")
        graph = nx.read_gexf(file_name)
        print('Graph Info: %s' % nx.info(graph))
    else:
        print('No existing graph file found')

    return graph


def generate_document_integer_graph_savetodb(integer_dict,
                                             document_graph,
                                             edge_dict):
    '''Generate compacted/Integer subgraph based on the integer dictionary.
        If the ops = a append to the configuration document graph'''

    '''Step1: Remove the nodes that is not present in the dictionary'''
    document_graph = remove_missing_nodes(integer_dict, document_graph)

    '''Step2: Integerize nodes'''
    graph = relabel_nodes(document_graph, integer_dict)

    '''Step3: Save into the file
    edge_file = open(dcrconfig.ConfigManager().DocumentsEdgesIntegerFile, 'a')
    print(document, file=edge_file)'''
    intGraph = ''
    for edge in graph.edges(data=True):
        edge1 = edge[0]
        edge2 = edge[1]
        if edge1 > edge2:
            edge1 = edge[1]
            edge2 = edge[0]
        key = edge1, edge2
        if key in edge_dict:
            # print('%d %d %d %d' % (edge_dict[key],
            #       edge1, edge2, edge[2]['weight']),
            #       file=edge_file)
            intGraph += str(edge_dict[key]) + ' ' + str(edge1) + ' ' + str(edge2) + ' ' + str(edge[2]['weight'])+','
    return intGraph


def generate_document_integer_graph_fromdb(signGraph,
                                           doc_id,
                                           file_operation='a',
                                           edge_file_path=''):

    signGraph = signGraph.strip()
    if signGraph[len(signGraph)-1] == ",":
        signGraph = signGraph[0:len(signGraph)-1]
    signGraph = signGraph.strip()
    signGraphLines = signGraph.split(',')
    doc_id_enhanced = '---' + str(int(doc_id)) + '---'
    edge_file = open(edge_file_path, file_operation)
    print(doc_id_enhanced, file=edge_file)
    for line in signGraphLines:
        elements = line.split(' ')
        edge = float(elements[0])
        node1 = float(elements[1])
        node2 = float(elements[2])
        weight = float(elements[3])
        print(edge, node1, node2, weight)
        print('%d %d %d %d' % (edge, node1, node2, weight), file=edge_file)
    edge_file.close()


def graph_to_signature_graph(integer_dict, document_graph, edge_dict):
    # Generate compacted/Integer subgraph based on the integer dictionary.
    # If the ops = a append to the configuration document graph

    # Step1: Remove the nodes that is not present in the dictionary
    document_graph = remove_missing_nodes_from_graph(integer_dict, document_graph)

    graph = relabel_nodes(document_graph, integer_dict)

    intGraph = ''
    graphDict = nx.get_edge_attributes(graph, 'weight')

    for edge in graphDict:
        sortedEdge = tuple(sorted(edge))
        if sortedEdge in edge_dict:
            intGraph += str(edge_dict[sortedEdge]) + ' ' + str(sortedEdge[0]) + ' ' + str(sortedEdge[1]) + ' ' + str(graphDict[edge])+','
    return intGraph


def remove_missing_nodes_from_graph(integer_dict, document_graph):
    nodes = document_graph.nodes()
    graphNodesDict = dict.fromkeys(nodes)
    missingKeys = set(graphNodesDict.keys()) - set(integer_dict.keys())
    document_graph.remove_nodes_from(missingKeys)
    return document_graph


def create_document_graph_distant_neighbors(phrase_string, neighborCount, diminition_percent, edge_weight=1):
    phrase_sentences = phrase_string.split('.')
    base_graph = create_graph(phrase_string.replace('.', ''))
    neighbor_sensitive_graph = nx.Graph()

    for sent in phrase_sentences:
        ph = sent.split('|')
        phrases = [s for s in ph if len(s) > 2]
        neighbor_sensitive_graph.add_nodes_from(phrases)

        neighborPhrasesList = list(product(enumerate(phrases), repeat =2))

        edge_weight = dcrconfig.ConfigManager().GraphEdgeWeight

        for neighbor in neighborPhrasesList:
            if (neighbor[0])[0] < (neighbor[1])[0]:
                neighborDistance = (neighbor[1])[0] - (neighbor[0])[0]
                if neighborDistance <= neighborCount:
                    if neighborDistance == 1:
                        edge_weight = dcrconfig.ConfigManager().GraphEdgeWeight
                    else:
                        edge_weight = math.floor(dcrconfig.ConfigManager().GraphEdgeWeight * (diminition_percent / 100) ** (neighborDistance-1))

                    neighbor_sensitive_graph.add_edge((neighbor[0])[1], (neighbor[1])[1], weight=edge_weight)

    union_graph(base_graph, neighbor_sensitive_graph)
    return base_graph


def neighbor_count_for_edge_weight():
    neighborCount = 0
    edge_weight = dcrconfig.ConfigManager().GraphEdgeWeight
    diminition_percent = dcrconfig.ConfigManager().DiminitionPercentage
    while True:
        neighborCount += 1
        edge_weight = math.floor(edge_weight * diminition_percent / 100)
        if edge_weight < 1:
            break
    return neighborCount


def create_graph_with_generator(phrase_string, edge_weight=1):
    ph = phrase_string.split('|')
    phrases = list((s for s in ph if len(s) > 2))
    graph = nx.Graph()
    graph.add_nodes_from(phrases)
    # Add all edges
    i = 0
    while i < (len(phrases) - 1):
        graph.add_edge(phrases[i], phrases[i + 1], weight=edge_weight)
        i += 1

    return graph


def create_graph_distant_neighbors_with_generator(phrase_string, edge_weight=1):
    phrase_sentences = phrase_string.split('.')
    base_graph = create_graph_with_generator(phrase_string.replace('.', ''))
    neighbor_sensitive_graph = nx.Graph()
    diminition_percent = dcrconfig.ConfigManager().DiminitionPercentage

    for sent in phrase_sentences:
        ph = sent.split('|')
        phrases = list((s for s in ph if len(s) > 2))
        neighbor_sensitive_graph.add_nodes_from(phrases)

        # Add all edges
        phrase_len = len(phrases)
        for i in range(phrase_len - 1):
            edge_weight = dcrconfig.ConfigManager().GraphEdgeWeight
            for j in range(i + 1, phrase_len):
                neighbor_sensitive_graph.add_edge(phrases[i],
                                                  phrases[j],
                                                  weight=edge_weight)

                #   Reduce the graph weight by the predefined percentage
                edge_weight = math.floor(edge_weight
                                         * diminition_percent / 100)
                #   If the edge_weight diminishes to less than 1,
                #   then you don't need to proceed.
                if edge_weight < 1:
                    break

    union_graph(base_graph, neighbor_sensitive_graph)
    return base_graph