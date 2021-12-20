import os
import IPy
import time
import psutil
import argparse
import datetime


basic_dir = '../'
log_file = './'
p_o_dict = dict()


class PrefixOrigin:
    def __init__(self, prefix, origin, status, roa, vps):
        self.prefix = prefix
        self.origin = origin
        self.status = status
        self.roa = roa
        self.vp_pathset = dict()
    
    def add_vps(self, vps):
        for vp in vps:
            self.vp_pathset[vp] = set()
    
    def unique(self, path):
        res_list = []
        as_in_path = path.split(' ')
        for item in as_in_path:
            if item in res_list:
                continue
            res_list.append(item)
        res_str = ''
        for item in res_list:
            res_str += item + ' '
        return res_str[:-1]
    
    def add_path(self, vp, path):
        if vp not in self.vp_pathset.keys():
            self.vp_pathset[vp] = set()
        # print_to_log('add path {}'.format(self.unique(path)))
        self.vp_pathset[vp].add(self.unique(path))


def print_to_log(message):
    with open(log_file, 'a') as output_log:
        print('[{}] {}'.format(datetime.datetime.now(), message), file=output_log)


def print_to_res(line, path):
    with open(path, 'a') as res_file:
        if line[-1] == '\n':
            line = line[:-1]
        print(line, file=res_file)


def get_version(prefix):
    if(':' in prefix):
        return 6
    else:
        return 4


def free_memory():
    while psutil.virtual_memory().available / 1024 / 1024 / 1024 < 20:
        print_to_log('memory free less than 20 GB ({}), sleep 30min'.format(psutil.virtual_memory().available / 1024 / 1024 / 1024))
        time.sleep(60 * 30)


def load_invalid(date_time, record_type):
    for version in [4, 6]:
        target_file = os.path.join(basic_dir, record_type, '{}_v{}.txt'.format(date_time, version))
        with open(target_file) as read_file:
            for line in read_file:
                elem = line.split('|')
                if len(elem) < 5 or '/' not in elem[0]:
                    continue
                if elem[2] not in ['2', '3', '4']:
                    continue
                prefix = elem[0]
                origin = elem[1]
                status = elem[2]
                roa = elem[3]
                vps = elem[4].split(' ')
                p_o_dict['{}|{}'.format(prefix, origin)] = PrefixOrigin(prefix, origin, status, roa, vps)


def is_valid(as_path):
    if('{' in as_path):
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
            return False
    return True


def process_line(line):
    try:
        elem = line.split('|')
        if len(elem) < 7:
            return
        prefix = elem[5]
        vp = elem[4]
        path = elem[6]
        as_in_path = path.split(' ')

        if(vp != as_in_path[0]):
            as_in_path.insert(0, vp)
            path = vp + ' ' + path
        
        if(not is_valid(path)):
            return

        try:
            IPy.IP(prefix)
        except Exception as e:
            # print_to_log('[ERROR] {}'.format(e), error_log)
            return

        if '/' not in prefix:
            # print_to_log('[ERROR] no "/" in prefix: {}'.format(line[:-1]), error_log)
            return

        origin = as_in_path[-1]
        if(origin == '0' or origin == '' or vp == '0' or vp == ''):
            print_to_log("[ERROR] origin and vp {}".format(line[:-1]))
            return

        p_o_key = '{}|{}'.format(prefix, origin)
        if p_o_key not in p_o_dict.keys():
            return
        
        p_o_dict[p_o_key].add_path(vp, path)

    except Exception as e:
        print_to_log('[ERROR] {} line: {}'.format(e, line[:-1]))


def process_file(path, cnt_file, total_file, record_type):
    print_to_log('[{}] ({}/{}) begin process {}'.format(record_type, cnt_file, total_file, path))
    with open(path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            process_line(line)


def process(date_time, record_type):
    target_dir = basic_dir + 'temp' + record_type.capitalize() + '/' + date_time + '/'
    file_list = os.listdir(target_dir)
    print_to_log('[{0}] Begin read {0} files...'.format(record_type))
    cnt_file = 0
    total_file = len(file_list)
    for file in file_list:
        cnt_file += 1
        if(not file.endswith('.txt')):
            continue
        process_file(os.path.join(target_dir, file), cnt_file, total_file, record_type)
        free_memory()
        # break


def write_file(date_time, record_type):
    print_to_log('begin write')
    read_file_v4 = os.path.join(basic_dir, record_type, '{}_v4.txt'.format(date_time))
    read_file_v6 = os.path.join(basic_dir, record_type, '{}_v6.txt'.format(date_time))
    with open(read_file_v4, 'r') as f:
        print_to_log('write v4')
        lines = f.readlines()
        for line in lines:
            elem = line.split('|')
            if len(elem) < 5 or '/' not in elem[0]:
                continue
            p_o_key = '{}|{}'.format(elem[0], elem[1])
            write_line = ''
            if p_o_key in p_o_dict.keys():
                item = p_o_dict[p_o_key]
                write_line = '{}|{}|{}|{}|'.format(item.prefix, item.origin, item.status, item.roa)
                for vp in item.vp_pathset.keys():
                    write_line += '{}:'.format(vp)
                    for path in item.vp_pathset[vp]:
                        write_line += '{},'.format(path)
                    write_line = write_line[:-1] + ':'
                write_line = write_line[:-1]
            else:
                for i in range(len(elem) - 1):
                    write_line += elem[i] + '|'
                for vp in elem[-1].split(' '):
                    write_line += vp + '::'
                write_line = write_line[:-2]
            print_to_res(write_line, os.path.join(basic_dir, record_type, '{}_v4_path.txt'.format(date_time)))
            free_memory()
    print_to_log('v4 done')

    with open(read_file_v6, 'r') as f:
        print_to_log('write v6')
        lines = f.readlines()
        for line in lines:
            elem = line.split('|')
            if len(elem) < 5 or '/' not in elem[0]:
                continue
            p_o_key = '{}|{}'.format(elem[0], elem[1])
            write_line = ''
            if p_o_key in p_o_dict.keys():
                item = p_o_dict[p_o_key]
                write_line = '{}|{}|{}|{}|'.format(item.prefix, item.origin, item.status, item.roa)
                for vp in item.vp_pathset.keys():
                    write_line += '{}:'.format(vp)
                    for path in item.vp_pathset[vp]:
                        write_line += '{},'.format(path)
                    write_line = write_line[:-1] + ':'
                write_line = write_line[:-1]
            else:
                for i in range(len(elem) - 1):
                    write_line += elem[i] + '|'
                for vp in elem[-1].split(' '):
                    write_line += vp + '::'
                write_line = write_line[:-2]
            print_to_res(write_line, os.path.join(basic_dir, record_type, '{}_v6_path.txt'.format(date_time)))
            free_memory()
    print_to_log('v6 done')

    print_to_log('write done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start',help='%Y-%m-%d')
    parser.add_argument('end',help='%Y-%m-%d')
    parser.add_argument('log', help='name of log file')
    parser.add_argument('-r','--ribs',help='process RIBs in all collectors',action='store_true')
    parser.add_argument('-u','--updates',help='process Updates in all collectors',action='store_true')
    args = parser.parse_args()
    
    start_time = datetime.datetime.strptime(args.start, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(args.end, '%Y-%m-%d')
    log_file = os.path.join(log_file, args.log)

    while(start_time <= end_time):
        date_time = datetime.datetime.strftime(start_time, '%Y-%m-%d')

        if args.updates:
            p_o_dict = dict()
            print_to_log('begin load invalid prefix-origin')
            load_invalid(date_time, 'updates')
            print_to_log('done, {} invalid prefix-origin pair'.format(len(p_o_dict.keys())))

            process(date_time, 'updates')
            
            write_file(date_time, 'updates')
        
        if args.ribs:
            p_o_dict = dict()
            print_to_log('begin load invalid prefix-origin')
            load_invalid(date_time, 'ribs')
            print_to_log('done, {} invalid prefix-origin pair'.format(len(p_o_dict.keys())))

            process(date_time, 'ribs')
            
            write_file(date_time, 'ribs')
        
        start_time += datetime.timedelta(days=1)