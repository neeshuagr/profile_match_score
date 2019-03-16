#!/usr/bin/python3.4
#   Generate nodes from edges file
#   Reads from edges file and creates a nodes and saves it into a file

# import networkx as nx
# import dcrgraph
# import dcrconfig
# import sys

edge_file = open('int_edges.txt', 'r')
node_file = open('int_nodes.txt', 'w')

nodes = set()
for line in edge_file:
    words = line.split()
    nodes.add(words[0])
    nodes.add(words[1])

print('Saving integer nodes to file ...')
for node in nodes:
    print("%s" % node, file=node_file)
