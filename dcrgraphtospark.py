"""dcrgraphtospark.py"""
from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("Graph Analytics engine").setMaster("local")

conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.scheduler.mode", "FAIR")
conf.set("spark.localExecution.enabled", "true")
conf.set("spark.eventLog.enabled", "true")
conf.set("spark.akka.frameSize", "500")
sc = SparkContext(conf=conf)

node_file = sc.textFile("/home/Data/nlpdata/int_nodes.txt")
edge_file = sc.textFile("/home/Data/nlpdata/int_edges.txt")


def create_edge(line):
    values = line.split(" ")
    return int(values[0]), int(values[1]), int(values[2])


def load_document_graphs():
    doc_edge_file = open("/home" + "/Data/nlpdata/integer_documents_edges1.txt")
    docid = ""
    doc_edge_count = 0
    doc_edges = []
    docs = []
    jdcount = 0

    for l in doc_edge_file:
        if (doc_edge_count != 0 and l.startswith("--")):
            doc_edge_count = 0
            docs.append((docid, list(doc_edges)))
            # print("Doc Id : %s" % docid)
            del doc_edges[:]

        if (l.startswith("--")):
            docid = l.strip().strip("-")
            jdcount += 1
            if (jdcount % 1000 == 0):
                print(jdcount),
        else:
            edge_values = l.split(" ")
            doc_edges.append((int(edge_values[0]),
                              int(edge_values[1]),
                              int(edge_values[2])))
            doc_edge_count += 1
    return docs


def docs_semantic_index(doc):
    global edges

    edge_list = [(y[0], y[1], y[2]) for x in doc[1]
                 for y in edges.value if((x[0] == y[0] and
                                          x[1] == y[1])or
                                         (x[0] == y[1] and
                                          x[1] == y[0]))]
    edge_index = sum([x[2] for x in edge_list])
    return edge_index  # edge_list


def p(x):
    # print(x[0], x[1], x[2])
    print(x)

nodes = node_file.map(lambda s: int(s))
edgesRDD = edge_file.map(create_edge)
edges = sc.broadcast(edgesRDD.collect())
print("-------------%s--------------" % type(edges))

print("%s nodes: %d  edges: %d  %s" % ("-" * 30, nodes.count(),
                                       1, "-" * 30))
print("%s : loading documents %s" % (datetime.now(), type(edges)))
docs = load_document_graphs()
docsRDD = sc.parallelize(docs)
docsRDD.cache()
indexes = docsRDD.map(docs_semantic_index)

indexes.foreach(p)

# print("# of Documents : %d %d" % (len(docs), indexes.count()))
print("%s : loading documents complete" % datetime.now())
input("Exiting...")
