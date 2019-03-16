#!/usr/bin/python3.4
import dcrnlp
import dcrgraphcompactor
import dcrgraph
import dcrconfig
import networkx as nx
from datetime import datetime
import sys


def generate_search_nodes(text):
    noun_phrases = dcrnlp.extract_nounphrases_sentences(text)
    integer_dict = dcrgraphcompactor.load_node_dict()
    graph = dcrgraph.create_graph_distant_neighbors(noun_phrases)
    dcrgraph.print_graph(graph)
    nodes = dcrgraph.generate_document_integer_nodes(integer_dict, graph)
    print('\ninput.nodes = ' + ' '.join(str(x) for x in nodes))


def integer_edges_from_dict(graph):
    int_dict = dcrgraphcompactor.load_node_dict()
    edges = []
    for v1, v2, w in graph.edges_iter(data=True):
        weight = int(w['weight'])
        n1 = int_dict[v1]
        n2 = int_dict[v2]
        if n1 > n2:
            n1 = n2
            n2 = int_dict[v1]
        edges.append([n1, n2, weight])
    return edges


def load_int_edges():
    int_edge_file = dcrconfig.ConfigManager().IntegerEdegesFile
    lines = [line.rstrip('\n').split(' ')
             for line in open(int_edge_file)]
    ilines = [list(map(int, l)) for l in lines]
    return ilines


def load_document_edges():
    edge_file = open(dcrconfig.ConfigManager().DocumentsEdgesIntegerFile, 'r')
    jdcount = 0

    docs = []
    doc_edges = []
    doc_id = 0
    for line in edge_file:
        line = line.strip()

        if (line.startswith('--')):
            #  If the line starts with -- then it is job descriptin beginning
            #  So print a dot indicate the progress
            if (len(doc_edges) > 0):
                doc = {'id': doc_id, 'edges': doc_edges}
                docs.append(doc)
                doc_edges = []
            doc_id = int(line.strip('-'))
            jdcount += 1

        if not (line.startswith('--') or len(line.strip()) < 1):
            doc_edges.append(int(line.split(' ')[0]))
    return docs


def get_common_edges(graph):
    edges = integer_edges_from_dict(graph)
    semantic_edges = load_int_edges()
    edge_ids = []
    for e, se in [(e, se) for e in edges for se in semantic_edges]:
        if e[0] == se[1] and e[1] == se[2]:
            edge_ids.append([se[0], se[3]])
    print(edge_ids)
    return edge_ids


def calculate_matching(edge_ids, docs):
    filtered_docs = []
    doc_count = 0
    for d in docs:
        edges = d['edges']
        filtered_edges = []
        for e in edges:
            fedges = [x for x in edge_ids if x[0] == e]
            fedges = [v for x in fedges for v in x]
            if (len(fedges) > 0):
                filtered_edges.append(fedges)
        if (len(filtered_edges) > 0):
            filtered_docs.append({'id': d['id'],
                                  'weighted_edge': filtered_edges})
            doc_count += 1
            # print(doc_count)
    return filtered_docs

if __name__ == "__main__":
    docs = load_document_edges()
    print(len(docs))
    # exit()
    text = """java"""
    # load_int_edges()
    generate_search_nodes(text)
    graph = dcrgraph.load_graph(dcrconfig.ConfigManager().SemanticGraphFile)
    node_list = list((u, v, d) for u, v, d in
                     graph.edges_iter([text],
                                      data=True) if (d['weight'] > 500))
    sub_graph = nx.Graph(node_list)

    print('sub graph  %s' % (nx.info(sub_graph)))
    dcrgraph.print_graph(sub_graph)
    print('Saving ..')
    # dcrgraph.draw_graph(sub_graph, datetime.now().strftime("%Y%m%d%H%M%S")
    #                    + '.pdf')
    edge_ids = get_common_edges(sub_graph)
    search_result = calculate_matching(edge_ids, docs)
    print('search count: %d' % len(search_result))

    f = open("search_result.txt", 'w')
    for s in search_result:
        print(s, file=f)
        # input()
