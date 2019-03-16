#!/usr/bin/python3.4


import networkx as nx
import dcrgraph
import dcrconfig
from datetime import datetime
import os.path
import sys
from pymongo import MongoClient


def load_graph():
    '''Load semantic graph if it is already present in the system'''
    semantic_graph = nx.Graph()
    semantic_graph_path = dcrconfig.ConfigManager().SemanticGraphFile
    if os.path.isfile(semantic_graph_path):
        print("File found")
        semantic_graph = nx.read_gexf(semantic_graph_path)
        print('Semantic Graph Info: %s' % nx.info(semantic_graph))
    else:
        print('No existing semantic graph found')

    return semantic_graph


def update_graph():
    '''Load the existing graph and update with new set of job description
    from predefined locations based on the application.ini file'''
    semantic_graph = load_graph()

    '''Get the config values'''
    graph_weight = dcrconfig.ConfigManager().GraphEdgeWeight
    graph_filter_weight = dcrconfig.ConfigManager().FilterGraphEdgeWeight
    print("weight:%d filter weight: %d" % (graph_weight, graph_filter_weight))

    # graph_collection = []
    jdcount = 0

    cl = MongoClient()
    db = cl['datadocs']
    col = db['datacollectiondetails']
    for doc in col.find():
        line = doc['nounPhrases']
        # doc_id = doc['doc_id']

        if not (len(line.strip()) < 1):
            graph = dcrgraph.create_graph_distant_neighbors(line, graph_weight)
            dcrgraph.union_graph(semantic_graph, graph, graph_weight)
            jdcount += 1
            print('.', end='')
            if jdcount % 1000 == 0:
                print('%d' % jdcount)
            sys.stdout.flush()

    count = list((d['weight']) for u, v, d in
                 semantic_graph.edges_iter(data=True)
                 if d['weight'] > graph_filter_weight)

    ''' nx.write_gexf(semantic_graph,
                    dcrconfig.ConfigManager().SemanticGraphFile)'''
    mx = max(d for d in count)
    print('mx : %d, total jd processed : %d ' % (mx, jdcount))
    print('Semantic Graph Info: %s' % nx.info(semantic_graph))
    return semantic_graph


'''Update the existing graph with new data'''
start = datetime.now()
sgraph = update_graph()

graph_filter_weight = dcrconfig.ConfigManager().FilterGraphEdgeWeight
node_list = list((u, v, d) for u, v, d in
                 sgraph.edges_iter(data=True)
                 if (d['weight'] > graph_filter_weight))

sub_graph = nx.Graph(node_list)
print('sub graph  %s' % (nx.info(sub_graph)))

print('writing the new gexf file')
nx.write_gexf(sub_graph, dcrconfig.ConfigManager().SemanticGraphFile
              + start.strftime("%Y%m%d%H%M%S"))
# dcrgraph.draw_graph(sub_graph, 'graph_New.pdf')


def empty_fun():
    '''
    node_list = list((u, v, d) for u, v, d in
                    semantic_graph.edges_iter(data=True)
                    if (d['weight'] > 200))

    sub_graph = nx.Graph(node_list)
    print('sub graph  %s' % (nx.info(sub_graph)))
    nx.write_gexf(sub_graph, 'sub_semantic_graph_200.gexf')

    while True:
        lo = int(input('Input the Lower limit: '))
        hi = int(input('Input the upper limit: '))
        if hi < 10:
            break

        node_list = list((u, v, d) for u, v, d in
                        semantic_graph.edges_iter(data=True)
                        if (d['weight'] > lo) and (d['weight'] < hi))

        sub_graph = nx.Graph(node_list)
        print('sub graph  %s' % (nx.info(sub_graph)))
        print(node_list)
        # dcrgraph.draw_graph(sub_graph, 'graph_' + str(i) + '.pdf')

    for i in range(100, 150, 5):
        print(i)

        node_list = list((u, v, d) for u, v, d in
                        semantic_graph.edges_iter(data=True)
                        if (d['weight'] > i) and (d['weight'] < i + 10))
        sub_graph = nx.Graph(node_list)
        print('sub graph %d - %d \n %s ' % (i, i+5, nx.info(sub_graph)),
            file=subgraph_file)
        print(node_list, file=subgraph_file)
        # dcrgraph.draw_graph(sub_graph, 'graph_' + str(i) + '.pdf')

    for i in range(max - 100, max, 10):
        print(i)
        sub_graph = nx.Graph(list((u, v, d) for u, v, d in
                            semantic_graph.edges_iter(data=True)
                            if (d['weight'] > i) and (d['weight'] < i + 10)))
        print('sub graph %d - %d \n %s ' % (i, i+10, nx.info(sub_graph)),
            file=subgraph_file)
        print(sub_graph, file=subgraph_file)


    # nx.write_gml(sub_graph, 'semanticgraph.gml')
    print('Density : %f  edge>50 :%d ' %
        (nx.density(semantic_graph), len(sub_graph)))

    print('semantic graph : %s' % nx.info(semantic_graph), file=subgraph_file)
    print('sub graph : %s' % nx.info(sub_graph), file=subgraph_file)

    # dcrgraph.draw_graph(sub_graph)
    # dcrgraph.print_graph(semantic_graph)
    '''
