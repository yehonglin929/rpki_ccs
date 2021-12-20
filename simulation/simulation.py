import os
import csv
import random
import argparse
from enum import Enum
from collections import deque


TOPO_PATH = './topo_20211001.txt'
LOG_PATH = None
TOP_PATH = None
VP_PATH = './nodeids/vp_nodeid_202109.txt'
BEGIN_NODE_ID = 1169
ROV = True
ROV_PERCENT = 0
MAX_PATH = [-1 for _ in range(10000)]
LOG = False


default = (0.2, 0.4, 0.6, 0.8, 1.0)


node_id_to_node = dict()
tier1_node_id = [1071, 1080, 422, 52, 57, 1099, 1150, 1154, 1163, 1165, 1169, 81, 1262, 1308, 102, 107, 108, 1348, 149]
stub_node_id = []
middle_node_id = []
top100 = []
vp = []

tree_node_id = set()
unreachable = set()
exclusive = set()


rov_nodes = set()
default_nodes = set()


class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.provider = set()
        self.peer = set()
        self.customer = set()
        self.current_path = None
        self.candidate_path = []
    
    def add_provider(self, provider):
        self.provider.add(provider)
    
    def add_peer(self, peer):
        self.peer.add(peer)
    
    def add_customer(self, customer):
        self.customer.add(customer)
    
    def add_candidate(self, path):
        self.candidate_path.append(path)
    
    def choose_best(self):
        if self.current_path != None:
            return
        best_path = MAX_PATH
        for candidate_path in self.candidate_path:
            if len(candidate_path) < len(best_path):
                best_path = candidate_path
            elif len(candidate_path) == len(best_path):
                if best_path[-1] > candidate_path[-1]:
                    best_path = candidate_path
        self.current_path = best_path
        self.candidate_path = []
    
    def get_default_neighbor(self):
        # find provider
        if len(self.provider) != 0:
            provider = []
            for p in self.provider:
                provider.append(p)
            res = int(random.random() * len(provider))
            return provider[res]
        # find peer
        elif len(self.peer) != 0:
            peer = []
            for p in self.peer:
                peer.append(p)
            res = int(random.random() * len(peer))
            return peer[res]
        # find customer
        else:
            customer = []
            for p in self.customer:
                customer.append(p)
            res = int(random.random() * len(customer))
            return customer[res]
    
    def get_type(self):
        if self.node_id in tier1_node_id:
            return 'tier1'
        if len(self.customer) == 0:
            return 'stub'
        return 'middle'
    
    def get_customer_num(self):
        return len(self.customer)


def print_to_log(message):
    open_path = os.path.join(LOG_PATH, '{}_rov{}.log'.format(BEGIN_NODE_ID, ROV_PERCENT))
    output_log = open(open_path, 'a')
    print(message, file=output_log)
    output_log.close()


def load_topo():
    lines = open(TOPO_PATH, 'r').readlines()
    for line in lines:
        elem = line.strip().split(',')
        node1 = int(elem[0])
        node2 = int(elem[1])
        rel = int(elem[2])

        # Init Node
        if node1 not in node_id_to_node.keys():
            node_id_to_node[node1] = Node(node1)
        if node2 not in node_id_to_node.keys():
            node_id_to_node[node2] = Node(node2)
        
        # Init Relationship
        if rel == -1:
            # node1: provider, node2: customer
            node_id_to_node[node1].add_customer(node2)
            node_id_to_node[node2].add_provider(node1)
        else:
            # node1: peer, node2: peer
            node_id_to_node[node1].add_peer(node2)
            node_id_to_node[node2].add_peer(node1)
    
    for node_id in node_id_to_node.keys():
        node_type = node_id_to_node[node_id].get_type()
        if node_type == 'tier1':
            pass
        elif node_type == 'stub':
            stub_node_id.append(node_id)
        else:
            middle_node_id.append(node_id)
    
    print('load topo done, {} nodes, {} tier1, {} stub, {} middle'.format(len(node_id_to_node.keys()), len(tier1_node_id), len(stub_node_id), len(middle_node_id)))
    if LOG:
        print_to_log('load topo done, {} nodes, {} tier1, {} stub, {} middle'.format(len(node_id_to_node.keys()), len(tier1_node_id), len(stub_node_id), len(middle_node_id)))


def bfs_provider():
    queue = deque([])
    queue.append(BEGIN_NODE_ID)
    current_layer = 1
    next_layer = 0
    visited = set()
    visited.add(BEGIN_NODE_ID)
    while queue:
        now_node_id = queue.popleft()
        now_node_path = node_id_to_node[now_node_id].current_path
        out_path = []
        for elem in now_node_path:
            out_path.append(elem)
        out_path.append(now_node_id)
        for provider_node_id in node_id_to_node[now_node_id].provider:
            if provider_node_id not in visited and provider_node_id not in rov_nodes:
                node_id_to_node[provider_node_id].add_candidate(out_path)
                queue.append(provider_node_id)
                next_layer += 1
                visited.add(provider_node_id)
                tree_node_id.add(provider_node_id)
        current_layer -= 1
        if current_layer == 0:
            current_layer = next_layer
            next_layer = 0
            for tree_node in tree_node_id:
                node_id_to_node[tree_node].choose_best()


def bfs_peer():
    peer_id_set = set()
    for current_node_id in tree_node_id:
        current_node_path = node_id_to_node[current_node_id].current_path
        out_path = []
        for elem in current_node_path:
            out_path.append(elem)
        out_path.append(current_node_id)
        # find all peers
        for peer_id in node_id_to_node[current_node_id].peer:
            if peer_id not in rov_nodes:
                node_id_to_node[peer_id].add_candidate(out_path)
                peer_id_set.add(peer_id)
    for peer_id in peer_id_set:
        tree_node_id.add(peer_id)
    for tree_node in tree_node_id:
        node_id_to_node[tree_node].choose_best()


def bfs_customer():
    queue = deque([])
    current_layer = 0
    next_layer = 0
    visited = set()
    for tree_node in tree_node_id:
        visited.add(tree_node)
        queue.append(tree_node)
        current_layer += 1
    while queue:
        now_node_id = queue.popleft()
        now_node_path = node_id_to_node[now_node_id].current_path
        out_path = []
        for elem in now_node_path:
            out_path.append(elem)
        out_path.append(now_node_id)
        for customer_node_id in node_id_to_node[now_node_id].customer:
            if customer_node_id not in visited and customer_node_id not in rov_nodes:
                node_id_to_node[customer_node_id].add_candidate(out_path)
                queue.append(customer_node_id)
                next_layer += 1
                visited.add(customer_node_id)
                tree_node_id.add(customer_node_id)
        current_layer -= 1
        if current_layer == 0:
            current_layer = next_layer
            next_layer = 0
            for tree_node in tree_node_id:
                node_id_to_node[tree_node].choose_best()


def init():
    global tree_node_id
    global unreachable
    tree_node_id = set()
    unreachable = set()
    for node_id in node_id_to_node.keys():
        node_id_to_node[node_id].current_path = None
        node_id_to_node[node_id].candidate_path = []
    tree_node_id.add(BEGIN_NODE_ID)
    node_id_to_node[BEGIN_NODE_ID].add_candidate([])
    node_id_to_node[BEGIN_NODE_ID].choose_best()


def cal_aver_path(reachable=None):
    sum_path = 0
    for tree_node in tree_node_id:
        sum_path += len(node_id_to_node[tree_node].current_path)
    if reachable != None:
        sum_path = 0
        for tree_node in reachable:
            sum_path += len(node_id_to_node[tree_node].current_path)
        if len(reachable) == 0:
            return -1
        else:
            return float(sum_path) / len(reachable)
    return float(sum_path) / len(tree_node_id)


def generate_rov_nodes():
    lines = open(TOP_PATH, 'r').readlines()
    for line in lines:
        top100.append(int(line.strip()))
    node_num = int(len(top100) * ROV_PERCENT)
    print('[ROV Node Num] {}'.format(node_num))
    for i in range(node_num):
        index = int(random.random() * len(top100))
        while top100[index] in rov_nodes:
            index = int(random.random() * len(top100))
        rov_nodes.add(top100[index])


def generate_default_nodes(default_route_percent):
    default_num = int(default_route_percent * (len(node_id_to_node.keys() - exclusive)))
    for i in range(default_num):
        default_node_id = int(random.random() * len(node_id_to_node.keys()))
        while default_node_id in default_nodes or default_node_id in exclusive:
            default_node_id = int(random.random() * len(node_id_to_node.keys()))
        default_nodes.add(default_node_id)
    # # tier1
    # tier1_default_num = default_route_percent[0]
    # for i in range(tier1_default_num):
    #     index = int(random.random() * len(tier1_node_id))
    #     while tier1_node_id[index] in default_nodes or tier1_node_id[index] in exclusive:
    #         index = int(random.random() * len(tier1_node_id))
    #     default_nodes.add(tier1_node_id[index])
    
    # # middle
    # middle_exclusive = 0
    # for ex in exclusive:
    #     if ex in middle_node_id:
    #         middle_exclusive += 1
    # middle_default_num = int(default_route_percent[1] * (len(middle_node_id) - middle_exclusive))
    # for i in range(middle_default_num):
    #     index = int(random.random() * len(middle_node_id))
    #     while middle_node_id[index] in default_nodes or middle_node_id[index] in exclusive:
    #         index = int(random.random() * len(middle_node_id))
    #     default_nodes.add(middle_node_id[index])
    
    # # stub
    # stub_exclusive = 0
    # for ex in exclusive:
    #     if ex in stub_node_id:
    #         stub_exclusive += 1
    # stub_default_num = int(default_route_percent[2] * (len(stub_node_id) - stub_exclusive))
    # for i in range(stub_default_num):
    #     index = int(random.random() * len(stub_node_id))
    #     while stub_node_id[index] in default_nodes or stub_node_id[index] in exclusive:
    #         index = int(random.random() * len(stub_node_id))
    #     default_nodes.add(stub_node_id[index])


def cal_default_route(default_route_percent):
    cal_nodes = set()
    reachable = set()
    for node_id in unreachable:
        if node_id in default_nodes:
            cal_nodes.add(node_id)
    before_cal_nodes = len(cal_nodes)
    while True:
        changed = False
        removed = set()
        for node_id in cal_nodes:
            neighbor_id = node_id_to_node[node_id].get_default_neighbor()
            if neighbor_id in tree_node_id or neighbor_id in reachable:
                new_path = [_ for _ in node_id_to_node[neighbor_id].current_path]
                new_path.append(neighbor_id)
                node_id_to_node[node_id].current_path = new_path
                reachable.add(node_id)
                removed.add(node_id)
                changed = True
        for node_id in removed:
            cal_nodes.remove(node_id)
        if not changed:
            break
    
    aver_path_len = cal_aver_path(reachable)
    
    print('Default Route Percent: {}, total unreachable: {}, unreachable_default_route: {}, {} reachable, aver_path_len of reachable = {}'.format(default_route_percent, len(unreachable), before_cal_nodes, len(reachable), aver_path_len))
    if LOG:
        print_to_log('Default Route Percent: {}, total unreachable: {}, unreachable_default_route: {}, {} reachable, aver_path_len of reachable = {}'.format(default_route_percent, len(unreachable), before_cal_nodes, len(reachable), aver_path_len))

    for node_id in reachable:
        node_id_to_node[node_id].current_path = None


def load_vp():
    open_file = open(VP_PATH, 'r')
    lines = open_file.readlines()
    for line in lines:
        vp.append(int(line.strip()))
    open_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--begin_node', help='The begin_node_id', type=int)
    parser.add_argument('-r', '--rov', help='Use ROV', action='store_true')
    parser.add_argument('-p', '--rov_percent', help='ROV Percent', type=float)
    parser.add_argument('-l', '--log', help='Print to log', action='store_true')
    parser.add_argument('-t', '--top', type=int)
    args = parser.parse_args()

    BEGIN_NODE_ID = args.begin_node
    ROV = args.rov
    ROV_PERCENT = args.rov_percent
    LOG = args.log
    LOG_PATH = './logs/1.3_202109_top{}_default/'.format(args.top)
    TOP_PATH = './nodeids/202109_top{}.txt'.format(args.top)

    os.system('mkdir {}'.format(LOG_PATH))

    print('[BEGIN_NODE: {}, ROV: {}, ROV_PERCENT: {}]'.format(BEGIN_NODE_ID, ROV, ROV_PERCENT))
    if LOG:
        print_to_log('[BEGIN_NODE: {}, ROV: {}, ROV_PERCENT: {}]'.format(BEGIN_NODE_ID, ROV, ROV_PERCENT))
    
    load_topo()

    # NO ROV
    # Init
    init()

    bfs_provider()
    bfs_peer()
    bfs_customer()

    # Get Exclusive
    for node_id in node_id_to_node.keys():
        if node_id not in tree_node_id:
            exclusive.add(node_id)
    
    aver_path_len = cal_aver_path()
    print('UNREACHABLE: {}'.format(len(exclusive)))
    print('AVERAGE_PATH_LENGTH: {}'.format(aver_path_len))
    if LOG:
        print_to_log('UNREACHABLE: {}'.format(len(exclusive)))
        print_to_log('AVERAGE_PATH_LENGTH: {}'.format(aver_path_len))
    
    # ROV
    if ROV:
        generate_rov_nodes()

        init()

        bfs_provider()
        bfs_peer()
        bfs_customer()

        for node_id in node_id_to_node.keys():
            if node_id not in tree_node_id and node_id not in exclusive:
                unreachable.add(node_id)
        
        # Get stub and vp unreachable number:
        # Get vps
        load_vp()
        # Cal stub_unreachable & vp_unreachable
        # Speedup
        stub_set = set()
        for s in stub_node_id:
            stub_set.add(s)
        # Begin
        stub_unreachable = 0
        vp_unreachable = 0
        for unreach in unreachable:
            if unreach in stub_set:
                stub_unreachable += 1
            if unreach in vp:
                vp_unreachable += 1
        # Cal stub_vp
        stub_vp = 0
        for s in stub_set:
            if s in vp:
                stub_vp += 1
        
        # Output
        print('UNREACHABLE: {}'.format(len(unreachable)))
        print('STUB_UNREACHABLE: {}'.format(stub_unreachable))
        print('VP_UNREACHABLE: {}'.format(vp_unreachable))
        print('STUB_VP: {}'.format(stub_vp))
        if LOG:
            print_to_log('UNREACHABLE: {}'.format(len(unreachable)))
            print_to_log('STUB_UNREACHABLE: {}'.format(stub_unreachable))
            print_to_log('VP_UNREACHABLE: {}'.format(vp_unreachable))
            print_to_log('STUB_VP: {}'.format(stub_vp))


        # Cal Default Route
        for default_route_percent in default:
            default_nodes = set()
            generate_default_nodes(default_route_percent)

            cal_default_route(default_route_percent)



