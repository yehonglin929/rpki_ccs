import os
import IPy
import csv
from sys import getsizeof
from collections import Mapping, Container
import datetime
import argparse
import psutil
from time import sleep

basic_dir = './'
basic_dir2 = '../'
error_log = './process_prefix_length_error.log'
roa_dir = os.path.join(basic_dir2, 'roas')
roa_file = ''
bin_min_max_4 = dict()
bin_min_max_6 = dict()
bad_len = set()
bad_asn = set()
bad_len_asn = set()
matched = set()

write_process = psutil.Process(os.getpid())

VALID = 0
UNKNOWN = 0
BAD_LEN = 0
BAD_ASN = 0
BAD_LEN_ASN = 0


def print_to_log(message, path='./process.log'):
    output_log = open(path, 'a')
    print('[{}] {}'.format(datetime.datetime.now(), message), file=output_log)
    output_log.close()


def print_to_res(line, path):
    res_file = open(path, 'a')
    print(line, file=res_file)
    res_file.close()


class Prefix_Origin:
    def __init__(self, prefix, origin):
        self.prefix = prefix
        self.origin = origin
        self.vps = set()
        self.status = -1 # 0: valid, 1: unknown, 2: bad_len, 3: bad_asn, 4: bad_len_asn
        self.roas = set()
    
    def unique(self, path):
        res_list = []
        for item in path:
            if item in res_list:
                continue
            res_list.append(item)
        res_str = ''
        for item in res_list:
            res_str += item + ' '
        return res_str[:-1]
    
    def add_vp(self, vp):
        self.vps.add(vp)
    
    def add_roa(self, roa_tuple):
        maxlength = roa_tuple[1]
        prefix = roa_tuple[3]
        asn = roa_tuple[2]
        self.roas.add('{}-{},AS{}'.format(prefix, maxlength, asn))


p_o_dict = dict()


def prefix_binary(prefix_addr, prefix_length):
    binary_str = IPy.IP(prefix_addr).strBin()
    return binary_str[:prefix_length]


def get_version(prefix):
    if(':' in prefix):
        return 6
    else:
        return 4


def load_roa(path):
    # print_to_log('load_roa begin - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
    reader = csv.reader(open(path, 'r'))
    cnt_row = 0
    for row in reader:
        cnt_row += 1
        if(cnt_row == 1):
            continue
        prefix = row[2]
        asn = row[1][2:]
        if asn == '0':
            continue
        maxlength = ''
        if(row[3] == ''):
            maxlength = int(prefix.split('/')[1])
        else:
            maxlength = int(row[3])
        prefix_addr = prefix.split('/')[0]
        prefix_length = int(prefix.split('/')[1])
        pre_bin = prefix_binary(prefix_addr, prefix_length)
        if(get_version(prefix) == 4):
            if(pre_bin not in bin_min_max_4.keys()):
                bin_min_max_4[pre_bin] = set()
            bin_min_max_4[pre_bin].add((prefix_length, maxlength, asn, prefix))
        else:
            if(pre_bin not in bin_min_max_6.keys()):
                bin_min_max_6[pre_bin] = set()
            bin_min_max_6[pre_bin].add((prefix_length, maxlength, asn, prefix))
    # print_to_log('load_roa end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))


def validate(anno_asn, anno_prefix):
    # print('validate begin - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
    anno_pre_addr = anno_prefix.split('/')[0]
    anno_pre_length = int(anno_prefix.split('/')[1])
    bin_anno_pre_addr = prefix_binary(anno_pre_addr, anno_pre_length)
    if(get_version(anno_prefix) == 4):
        for i in range(len(bin_anno_pre_addr)):
            test_pre_bin = bin_anno_pre_addr[:i+1]
            if(test_pre_bin in bin_min_max_4.keys()):
                for roa_tuple in bin_min_max_4[test_pre_bin]:
                    roa_maxlength = roa_tuple[1]
                    roa_asn = roa_tuple[2]
                    if(anno_pre_length > roa_maxlength):
                        if(anno_asn != roa_asn):
                            bad_len_asn.add(roa_tuple)
                        else:
                            bad_len.add(roa_tuple)
                    elif(anno_asn != roa_asn):
                        bad_asn.add(roa_tuple)
                    else:
                        matched.add(roa_tuple)
                        # print('validate end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
                        return
                    # break
    else:
        for i in range(len(bin_anno_pre_addr)):
            test_pre_bin = bin_anno_pre_addr[:i+1]
            if(test_pre_bin in bin_min_max_6.keys()):
                test_pre_bin = bin_anno_pre_addr[:i+1]
                for roa_tuple in bin_min_max_6[test_pre_bin]:
                    roa_maxlength = roa_tuple[1]
                    roa_asn = roa_tuple[2]
                    if(anno_pre_length > roa_maxlength):
                        if(anno_asn != roa_asn):
                            bad_len_asn.add(roa_tuple)
                        else:
                            bad_len.add(roa_tuple)
                    elif(anno_asn != roa_asn):
                        bad_asn.add(roa_tuple)
                    else:
                        matched.add(roa_tuple)
                        # print('validate end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
                        return
                # break
    # print('validate end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))


def get_status():
    if(len(matched) == 0):
        if(len(bad_len) == 0 and len(bad_asn) == 0 and len(bad_len_asn) == 0):
            return 1
        elif(len(bad_len) != 0):
            return 2
        elif(len(bad_asn) != 0):
            # print('bad_asn,', bad_asn)
            return 3
        elif(len(bad_len_asn) != 0):
            return 4
    else:
        return 0


def is_valid(as_path, line):
    # print_to_log('is_valid begin - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
    if('{' in as_path):
        # print_to_log(line, 'as_path_with_set.txt')
        # print_to_log('is_valid end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
        return False
    elems = as_path.split(' ')
    last_as = ''
    appeared_as = set()
    for elem in elems:
        if(elem not in appeared_as):
            appeared_as.add(elem)
            last_as = elem
        elif(last_as == elem): # Appeared more than once
            continue
        else: # Cycle
            # print_to_log('is_valid end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
            return False
    # print_to_log('is_valid end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
    return True


def get_prefix_origin_vp(line):
    global p_o_dict
    global VALID
    global UNKNOWN
    global BAD_ASN
    global BAD_LEN
    global BAD_LEN_ASN
    global bad_asn
    global bad_len
    global bad_len_asn
    global matched
    try:
        # print_to_log('get_prefix_origin_vp begin - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
        elems = line.split('|')
        if(len(elems) < 7):
            return
        vp = elems[4]
        prefix = elems[5]
        as_path = elems[6]
        as_in_path = as_path.split(' ')
        if(vp != as_in_path[0]):
            as_in_path.insert(0, vp)
            as_path = vp + ' ' + as_path
        if(not is_valid(as_path, as_in_path)):
            return
        try:
            IPy.IP(prefix)
        except Exception as e:
            print_to_log('[ERROR] {}'.format(e), error_log)
            return
        if '/' not in prefix:
            print_to_log('[ERROR] no "/" in prefix: {}'.format(line[:-1]), error_log)
        origin = as_in_path[-1]
        if(origin == '0' or origin == '' or vp == '0' or vp == ''):
            print_to_log("[ERROR] origin and vp {}".format(line[:-1]))
        p_o_key = prefix + origin
        if(p_o_key not in p_o_dict.keys()):
            p_o_dict[p_o_key] = Prefix_Origin(prefix, origin)
            validate(origin, prefix)
            p_o_dict[p_o_key].status = get_status()
            if(p_o_dict[p_o_key].status == 2): # bad_len
                BAD_LEN += 1
                for roa_tuple in bad_len:
                    p_o_dict[p_o_key].add_roa(roa_tuple)
            elif(p_o_dict[p_o_key].status == 3): # bad_asn
                BAD_ASN += 1
                for roa_tuple in bad_asn:
                    p_o_dict[p_o_key].add_roa(roa_tuple)
            elif(p_o_dict[p_o_key].status == 4): # bad_len_asn
                BAD_LEN_ASN += 1
                for roa_tuple in bad_len_asn:
                    p_o_dict[p_o_key].add_roa(roa_tuple)
            elif(p_o_dict[p_o_key].status == 0): # valid
                VALID += 1
                for roa_tuple in matched:
                    p_o_dict[p_o_key].add_roa(roa_tuple)
            elif(p_o_dict[p_o_key].status == 1): # unknown
                UNKNOWN += 1
            bad_len = set()
            bad_asn = set()
            bad_len_asn = set()
            matched = set()
        p_o_dict[p_o_key].add_vp(vp)
        # print_to_log('get_prefix_origin_vp end - memory: {}'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
    except Exception as e:
        print_to_log('[ERROR] {}, {}'.format(e, line[:-1]))
        return


def process_file(path, cnt_file, total_file, record_type):
    print_to_log('[{}] ({}/{}) begin process {}'.format(record_type, cnt_file, total_file, path))
    with open(path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            get_prefix_origin_vp(line)
    print_to_log('[{}] {} done, used memory:{} GB'.format(record_type, path, write_process.memory_info().rss / 1024 / 1024 / 1024))


def process(date_time, record_type):
    target_dir = basic_dir2 + 'temp' + record_type.capitalize() + '/' + date_time + '/'
    file_list = os.listdir(target_dir)
    print_to_log('[{0}] Begin read {0} files...'.format(record_type))
    cnt_file = 0
    total_file = len(file_list)
    for file in file_list:
        cnt_file += 1
        if(not file.endswith('.txt')):
            continue
        process_file(os.path.join(target_dir, file), cnt_file, total_file, record_type)
        print_to_log('# of items: {}'.format(len(p_o_dict.keys())))
        # break


def write_file(date_time, record_type):
    global p_o_dict
    print_to_log('[{}] Begin write {} files...'.format(record_type, date_time))
    target_dir = os.path.join(basic_dir2, record_type)
    print_to_log('pid = {}'.format(os.getpid()))

    for p_o_key in p_o_dict.keys():
        item = p_o_dict[p_o_key]
        line = '{}|{}|{}|'.format(item.prefix, item.origin, item.status)
        for roa in item.roas:
            line += roa + ' '
        if(len(item.roas) == 0):
            line += ' '
        line = line[:-1] + '|'
        # prefix | origin | status | roa | vp1 vp2 ...
        for vp in item.vps:
            line += vp + ' '
        if '/' not in line.split('|')[0]:
            print('[WRITE ERROR] / not in line: {}'.format(line))
        if(get_version(item.prefix) == 4):
            print_to_res(line.strip(), os.path.join(target_dir, date_time + '_v4.txt'))
        else:
            print_to_res(line.strip(), os.path.join(target_dir, date_time + '_v6.txt'))
        
        while (write_process.memory_info().rss / 1024 / 1024 / 1024) > 90:
            print_to_log('used memory: {} GB, sleep 10s'.format(write_process.memory_info().rss / 1024 / 1024 / 1024))
            sleep(10)
    print_to_log('write done')
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start',help='%Y-%m-%d')
    parser.add_argument('end',help='%Y-%m-%d')
    parser.add_argument('-r','--ribs',help='process RIBs in all collectors',action='store_true')
    parser.add_argument('-u','--updates',help='process Updates in all collectors',action='store_true')
    args = parser.parse_args()

    start_time = datetime.datetime.strptime(args.start, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(args.end, '%Y-%m-%d')

    while(start_time <= end_time):
        # next_day = start_time + datetime.timedelta(days=1)
        # next_day_standard = getStrDate(next_day)
        # while(not os.path.exists(basic_dir2 + 'tempUpdates/' + next_day_standard + '/') or not os.path.exists(basic_dir2 + 'tempRibs/' + next_day_standard + '/')):
        #     print_to_log('sleep 10s')
        #     sleep(10)
        date_time = datetime.datetime.strftime(start_time, '%Y-%m-%d')
        
        print_to_log('{} begin load roa'.format(date_time))
        load_roa(os.path.join(roa_dir, date_time, 'roas.csv'))
        # load_roa(os.path.join(roa_dir, '2021-05-02', 'roas.csv'))
        print_to_log('{} load roa done'.format(date_time))

        print_to_log('Begin process {}'.format(date_time))

        if(args.updates):
            p_o_dict = dict()
            print_to_log('Processing updates')
            process(date_time, 'updates')

            # os.chdir(os.path.join(basic_dir2, date_time))
            # os.system('rm -rf {}'.format(date_time))

            write_file(date_time, 'updates')

            print_to_log('{} updates done'.format(date_time))
            print_to_log('{} VALID = {}'.format(date_time, VALID))
            print_to_log('{} UNKNOWN = {}'.format(date_time, UNKNOWN))
            print_to_log('{} BAD_LEN = {}'.format(date_time, BAD_LEN))
            print_to_log('{} BAD_ASN = {}'.format(date_time, BAD_ASN))
            print_to_log('{} BAD_LEN_ASN = {}'.format(date_time, BAD_LEN_ASN))
            VALID = 0
            UNKNOWN = 0
            BAD_LEN = 0
            BAD_ASN = 0
            BAD_LEN_ASN = 0
        
        if(args.ribs):
            p_o_dict = dict()
            print_to_log('Processing ribs')
            process(date_time, 'ribs')

            # os.chdir(os.path.join(basic_dir2, date_time))
            # os.system('rm -rf {}'.format(date_time))

            write_file(date_time, 'ribs')

            print_to_log('{} ribs done'.format(date_time))
            print_to_log('{} VALID = {}'.format(date_time, VALID))
            print_to_log('{} UNKNOWN = {}'.format(date_time, UNKNOWN))
            print_to_log('{} BAD_LEN = {}'.format(date_time, BAD_LEN))
            print_to_log('{} BAD_ASN = {}'.format(date_time, BAD_ASN))
            print_to_log('{} BAD_LEN_ASN = {}'.format(date_time, BAD_LEN_ASN))
            VALID = 0
            UNKNOWN = 0
            BAD_LEN = 0
            BAD_ASN = 0
            BAD_LEN_ASN = 0
        
        start_time += datetime.timedelta(days=1)




