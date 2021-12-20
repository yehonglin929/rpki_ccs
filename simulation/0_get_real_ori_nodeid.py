import os
import csv


ASN_NODEID_PATH = '/home/yhl/ns3/ns-allinone-3.34/ns-3.34/scratch/asn_nodeId_20211001.csv'
REAL_ORI_PATH = '/home/yhl/Code/final_res/origin_AS.csv'
RES_PATH = '/home/yhl/Code/final_res/nodeids/real_ori_2021_09.txt'
ERROR_PATH = '/home/yhl/Code/final_res/0_real_ori_2021_09.txt'


real_ori = set()
asn_nodeid = dict()


def print_to_res(message, path=RES_PATH):
    output_file = open(path, 'a')
    print(message, file=output_file)
    output_file.close()


def load_top():
    open_file = open(REAL_ORI_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        if len(row) == 1:
            real_ori.add(row[0])
    open_file.close()


def load_asn_nodeid():
    open_file = open(ASN_NODEID_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        asn = row[0]
        nodeid = row[1]
        asn_nodeid[asn] = nodeid
        if asn in real_ori:
            print_to_res(nodeid)

    open_file.close()



if __name__ == '__main__':
    load_top()
    print(len(real_ori))
    load_asn_nodeid()

    for t in real_ori:
        if t not in asn_nodeid.keys():
            print_to_res(t, ERROR_PATH)


