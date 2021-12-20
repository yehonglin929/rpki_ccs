import os
import csv


ASN_NODEID_PATH = '/home/yhl/ns3/ns-allinone-3.34/ns-3.34/scratch/asn_nodeId_20211001.csv'
VP_PATH = '/home/yhl/Code/final_res/vp-all.csv'
RES_PATH = '/home/yhl/Code/final_res/nodeids/vp_nodeid_202109.txt'


vp = []
asn_nodeid = dict()


def print_to_res(message):
    output_file = open(RES_PATH, 'a')
    print(message, file=output_file)
    output_file.close()


def load_top():
    open_file = open(VP_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        vp.append(row[0])


def load_asn_nodeid():
    open_file = open(ASN_NODEID_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        asn = row[0]
        nodeid = row[1]
        asn_nodeid[asn] = nodeid
        if asn in vp:
            print_to_res(nodeid)

    open_file.close()



if __name__ == '__main__':
    load_top()
    print(len(vp))
    load_asn_nodeid()

    for t in vp:
        if t not in asn_nodeid.keys():
            print(t)


