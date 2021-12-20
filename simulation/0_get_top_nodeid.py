import os
import csv


ASN_NODEID_PATH = '/home/yhl/ns3/ns-allinone-3.34/ns-3.34/scratch/asn_nodeId_20211001.csv'
TOP_PATH = '/home/yhl/Code/final_res/top_asn/202109_top10000.txt'
RES_PATH = '/home/yhl/Code/final_res/nodeids/202109_top10000.txt'


top = []
asn_nodeid = dict()


def print_to_res(message):
    output_file = open(RES_PATH, 'a')
    print(message, file=output_file)
    output_file.close()


def load_top():
    open_file = open(TOP_PATH, 'r')
    lines = open_file.readlines()
    for line in lines:
        elem = line.strip().split('\t')
        top.append(elem[1])


def load_asn_nodeid():
    open_file = open(ASN_NODEID_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        asn = row[0]
        nodeid = row[1]
        asn_nodeid[asn] = nodeid
        if asn in top:
            print_to_res(nodeid)

    open_file.close()



if __name__ == '__main__':
    load_top()
    load_asn_nodeid()

    for t in top:
        if t not in asn_nodeid.keys():
            print(t)


