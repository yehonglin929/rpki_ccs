import os
import IPy
import csv


def get_version(prefix):
    if(':' in prefix):
        return 6
    else:
        return 4


class ROA:
    def __init__(self):
        pass
    
    def prefix_binary(self, prefix_addr, prefix_length):
        binary_str = IPy.IP(prefix_addr).strBin()
        return binary_str[:prefix_length]

    def load_roa(self, roa_path):
        # init
        # For real-time BGP validation and prefix search
        self.bin_min_max_4 = dict()
        self.bin_min_max_6 = dict()

        file_list = os.listdir(roa_path)
        for file in file_list:
            reader = csv.reader(open(os.path.join(roa_path, file), 'r'))
            cnt_row = 0
            for row in reader:
                # Skip Header
                cnt_row += 1
                if cnt_row == 1:
                    continue
                # Begin
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
                pre_bin = self.prefix_binary(prefix_addr, prefix_length)

                # For real-time BGP validation and prefix search
                if get_version(prefix) == 4:
                    if(pre_bin not in self.bin_min_max_4.keys()):
                        self.bin_min_max_4[pre_bin] = set()
                    self.bin_min_max_4[pre_bin].add((prefix_length, maxlength, asn, prefix))
                else:
                    if(pre_bin not in self.bin_min_max_6.keys()):
                        self.bin_min_max_6[pre_bin] = set()
                    self.bin_min_max_6[pre_bin].add((prefix_length, maxlength, asn, prefix))

    def cover_pfx(self, pfx):
        anno_pfx_addr = pfx.split('/')[0]
        anno_pfx_length = int(pfx.split('/')[1])
        bin_anno_pfx_addr = self.prefix_binary(anno_pfx_addr, anno_pfx_length)

        # Find cover
        cover = set()
        if get_version(pfx) == 4:
            for i in range(len(bin_anno_pfx_addr)):
                test_pfx_bin = bin_anno_pfx_addr[:i+1]
                if test_pfx_bin in self.bin_min_max_4.keys():
                    for roa_tuple in self.bin_min_max_4[test_pfx_bin]:
                        roa_maxlength = roa_tuple[1]
                        roa_asn = roa_tuple[2]
                        if anno_pfx_length <= roa_maxlength:
                            cover.add(roa_asn)
        else:
            for i in range(len(bin_anno_pfx_addr)):
                test_pfx_bin = bin_anno_pfx_addr[:i+1]
                if test_pfx_bin in self.bin_min_max_6.keys():
                    for roa_tuple in self.bin_min_max_6[test_pfx_bin]:
                        roa_maxlength = roa_tuple[1]
                        roa_asn = roa_tuple[2]
                        if anno_pfx_length <= roa_maxlength:
                            cover.add(roa_asn)
        return cover
    
    def validate(self, pfx, asn):
        '''
        pfx, asn should be str format
        '''
        bad_len = set()
        bad_asn = set()
        bad_len_asn = set()
        matched = set()

        anno_pfx_addr = pfx.split('/')[0]
        anno_pfx_length = int(pfx.split('/')[1])
        bin_anno_pfx_addr = self.prefix_binary(anno_pfx_addr, anno_pfx_length)

        # validate
        if get_version(pfx) == 4:
            for i in range(len(bin_anno_pfx_addr)):
                test_pfx_bin = bin_anno_pfx_addr[:i+1]
                if test_pfx_bin in self.bin_min_max_4.keys():
                    for roa_tuple in self.bin_min_max_4[test_pfx_bin]:
                        roa_maxlength = roa_tuple[1]
                        roa_asn = roa_tuple[2]
                        if anno_pfx_length > roa_maxlength:
                            if asn != roa_asn:
                                bad_len_asn.add(roa_tuple)
                            else:
                                bad_len.add(roa_tuple)
                        elif asn != roa_asn:
                            bad_asn.add(roa_tuple)
                        else:
                            matched.add(roa_tuple)
        else:
            for i in range(len(bin_anno_pfx_addr)):
                test_pfx_bin = bin_anno_pfx_addr[:i+1]
                if test_pfx_bin in self.bin_min_max_6.keys():
                    for roa_tuple in self.bin_min_max_6[test_pfx_bin]:
                        roa_maxlength = roa_tuple[1]
                        roa_asn = roa_tuple[2]
                        if anno_pfx_length > roa_maxlength:
                            if asn != roa_asn:
                                bad_len_asn.add(roa_tuple)
                            else:
                                bad_len.add(roa_tuple)
                        elif asn != roa_asn:
                            bad_asn.add(roa_tuple)
                        else:
                            matched.add(roa_tuple)
        
        # get status
        if len(matched) == 0:
            if len(bad_len) == 0 and len(bad_asn) == 0 and len(bad_len_asn) == 0:
                return 'Unknown', set()
            elif len(bad_len) != 0:
                return 'Bad_len', bad_len
            elif len(bad_asn) != 0:
                return 'Bad_asn', bad_asn
            elif len(bad_len_asn) != 0:
                return 'Bad_len_asn', bad_len_asn
        else:
            return 'Valid', matched
