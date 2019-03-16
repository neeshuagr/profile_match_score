#!/usr/bin/python3.4
import dcrgraphcompactor
import csv


if __name__ == "__main__":
    dict = dcrgraphcompactor.load_node_dict_file_path('/mnt/nlpdata/nodedict.txt')  # dcrgraphcompactor.load_node_dict()
    csvfile = csv.writer(open("/mnt/nlpdata/nodedict.csv", "w"))
    for key, value in dict.items():
        csvfile.writerow([key, value])
