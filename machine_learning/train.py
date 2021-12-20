import os
import csv
import random
import pickle
import argparse
import datetime
import _validate
import multiprocessing
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedKFold
from imblearn.over_sampling import RandomOverSampler


KFOLD = 10
LOG_PATH = None
TRAIN_P_N = None
MONTH_PATH = None
CLASSIFIER = None
MODULE_PATH = None
PREDICT_MONTH = False
CROSS_VALIDATION_RES_PATH = None
CROSS_VALIDATION = False
SELECT_ATTRIBUTES = set()
DATASET_DIR = './dataset/'
ROA_DIR = './dataset/roas/'


date_list = (
    ('2020', '03'), ('2020', '05'), ('2020', '07'), ('2020', '09'), ('2020', '11'),
    ('2021', '01'), ('2021', '03'), ('2021', '05'), ('2021', '07'), ('2021', '09')
)
pred_month = (
    ('2017', '01'), ('2017', '05'), ('2017', '09'),
    ('2018', '01'), ('2018', '05'), ('2018', '09'),
    ('2019', '01'), ('2019', '05'), ('2019', '09'),
    ('2020', '01'), ('2020', '03'), ('2020', '05'), ('2020', '07'), ('2020', '09'), ('2020', '11'),
    ('2021', '01'), ('2021', '03'), ('2021', '05'), ('2021', '07'), ('2021', '09'), ('2021', '11')
)
date_event = dict()
x = []
y = []


def log(message):
    output_log = open(LOG_PATH, 'a')
    print('[{}] {}'.format(datetime.datetime.now(), message), file=output_log)
    output_log.close()


def get_select_attributes(args):
    if args.last_days:
        SELECT_ATTRIBUTES.add('last_days')
    if args.rel:
        SELECT_ATTRIBUTES.add('rel')
    if args.org:
        SELECT_ATTRIBUTES.add('org')
    if args.vp_num:
        SELECT_ATTRIBUTES.add('vp_num')
    if args.invalid_type:
        SELECT_ATTRIBUTES.add('invalid_type')
    if args.vrp_num:
        SELECT_ATTRIBUTES.add('vrp_num')
    if args.vrp_as_num:
        SELECT_ATTRIBUTES.add('vrp_as_num')
    if args.vp_received_num:
        SELECT_ATTRIBUTES.add('vp_received_num')
    if args.self_pfx_num:
        SELECT_ATTRIBUTES.add('self_pfx_num')
    if args.other_pfx_num:
        SELECT_ATTRIBUTES.add('other_pfx_num')
    if args.self_valid_pfx_num:
        SELECT_ATTRIBUTES.add('self_valid_pfx_num')
    if args.other_valid_pfx_num:
        SELECT_ATTRIBUTES.add('other_valid_pfx_num')
    if args.cover_asn:
        SELECT_ATTRIBUTES.add('cover_asn')


def load_last_time(date):
    event_lastDay = dict()
    data_path = os.path.join(DATASET_DIR, '{}-{}_v4_vp_lastTime_new.csv'.format(date[0], date[1]))
    data_file = open(data_path, 'r')
    reader = csv.reader(data_file)
    for row in reader:
        event = '{}, AS{}'.format(row[0], row[1])
        last_days = int(row[2])
        appear_days = [int(item) for item in row[3].split(' ')]
        event_lastDay[event] = (last_days, min(appear_days))
    data_file.close()
    return event_lastDay


def get_last_month(date):
    last_month_date_list = []
    current_date = datetime.datetime.strptime('{}-{}-01'.format(date[0], date[1]), '%Y-%m-%d')
    current_date -= datetime.timedelta(days=1)
    month = current_date.month
    while month == current_date.month:
        last_month_date_list.insert(0, datetime.datetime.strftime(current_date, '%Y-%m-%d'))
        current_date -= datetime.timedelta(days=1)
    return last_month_date_list


def get_last_day(date):
    current_date = datetime.datetime.strptime('{}-{}-01'.format(date[0], date[1]), '%Y-%m-%d')
    month = current_date.month
    while month == current_date.month:
        current_date += datetime.timedelta(days=1)
    current_date -= datetime.timedelta(days=1)
    return datetime.datetime.strftime(current_date, '%Y-%m-%d')


def get_asn_roa_activity(date):
    # ASN_ROA_Activity:
    #       calculate AS frequency of change its own ROA
    #       uses Last whole month, e.g. the dataset is March, then ROA uses [Feb.1th, Mar.1th)
    last_month_date_list = get_last_month(date)
    asn_roa_activity = dict()
    last_day_asn_vrp = dict()
    today_asn_vrp = dict()

    for last_month_date in last_month_date_list:
        roa_path = os.path.join(ROA_DIR, last_month_date, 'roas.csv')
        roa_file = open(roa_path, 'r')
        reader = csv.reader(roa_file)
        for row in reader:
            asn = int(row[1][2:])
            if asn not in today_asn_vrp.keys():
                today_asn_vrp[asn] = set()
            today_asn_vrp[asn].add(tuple(row[:4]))
        
        for asn in today_asn_vrp.keys():
            if asn not in last_day_asn_vrp.keys():
                asn_roa_activity[asn] = 0
            elif len(last_day_asn_vrp[asn]) != len(today_asn_vrp[asn]) or len(last_day_asn_vrp[asn].union(today_asn_vrp[asn])) != len(today_asn_vrp[asn]):
                if last_month_date.split('-')[-1] != '01':
                    asn_roa_activity[asn] += 1
        last_day_asn_vrp = today_asn_vrp
        today_asn_vrp = dict()
    
    return asn_roa_activity


def get_asn_moas_activity(date):
    # ASN_MOAS_Activity:
    #       calculates the num of ASN contained in the MOAS set with that AS
    #       uses the last day of that month,
    last_day = '{}-{}-01'.format(date[0], date[1])
    asn_moas_activity = dict()

    roa = _validate.ROA()
    roa_dir = os.path.join(ROA_DIR, last_day)
    roa_path = os.path.join(roa_dir, 'roas.csv')
    roa.load_roa(roa_dir)
    
    roa_file = open(roa_path, 'r')
    reader = csv.reader(roa_file)
    for row in reader:
        asn = int(row[1][2:])
        pfx = row[2]
        cover = roa.cover_pfx(pfx)

        if asn not in asn_moas_activity.keys():
            asn_moas_activity[asn] = set()
        for cover_asn in cover:
            if int(cover_asn) != asn:
                asn_moas_activity[asn].add(int(cover_asn))
                if int(cover_asn) not in asn_moas_activity.keys():
                    asn_moas_activity[int(cover_asn)] = set()
                asn_moas_activity[int(cover_asn)].add(asn)
    return asn_moas_activity


def get_pfx_cover(date):
    last_day = '{}-{}-01'.format(date[0], date[1])
    roa = _validate.ROA()
    roa_dir = os.path.join(ROA_DIR, last_day)
    roa.load_roa(roa_dir)
    return roa


def load_samples():
    log('Load Samples.')
    for date in date_list:
        event_lastDay = load_last_time(date)

        # 10.25 Add
        # ASN_ROA_Activity, ASN_MOAS_Activity, pfx_cover (3 attributes)
        # 
        # ASN_ROA_Activity:
        #       calculate AS frequency of change its own ROA
        # ASN_MOAS_Activity:
        #       calculates the num of ASN contained in the MOAS set with that AS
        # pfx_cover:
        #       calculates the num of AS that can announce the pfx simutaneously
        # 
        # ROA_Activity uses Last whole month, 
        #       e.g. the dataset is March, then ROA uses [Feb.1th, Mar.1th)
        # MOAS_Activity & pfx_cover uses the last day of that month,
        #       e.g. the dataset is March, then ROA uses Mar.31th.
        # asn_roa_activity = get_asn_roa_activity(date)
        # asn_moas_activity = get_asn_moas_activity(date)
        roa = get_pfx_cover(date)

        if date not in date_event.keys():
            date_event[date] = []
        data_path = os.path.join(DATASET_DIR, '{}-{}_1.csv'.format(date[0], date[1]))
        data_file = open(data_path, 'r')
        reader = csv.reader(data_file)
        for row in reader:
            last_days = int(row[18])
            event = '{}, AS{}'.format(row[0], row[1])
            if event in event_lastDay.keys():
                last_days = event_lastDay[event][0]
            rel_self = False
            rel_p2c = False
            rel_c2p = False
            rel_p2p = False
            if '0' in row[5]:
                rel_p2p = True
            if '1' in row[5]:
                rel_c2p = True
            if '-1' in row[5]:
                rel_p2c = True
            if row[5] == '':
                rel_self = True
            same_org = False
            if row[24] == 'theSame':
                same_org = True
            vp_num = int(row[20])
            invalid_type = int(row[2])
            vrp_num = len(row[3].strip().split(' '))
            vrp_as = set()
            vrps = row[3].strip().split(' ')
            for vrp in vrps:
                asn = vrp.split(',AS')[-1]
                vrp_as.add(asn)
            vrp_as_num = len(vrp_as)
            vp_received_num = 0
            if row[4] != '#':
                vp_received_num = len(row[4].strip().split(' '))
            
            # sub/sup prefix
            # num
            self_subpfx_num = 0
            self_suppfx_num = 0
            other_subpfx_num = 0
            other_suppfx_num = 0
            self_valid_subpfx_num = 0
            self_valid_suppfx_num = 0
            other_valid_subpfx_num = 0
            other_valid_suppfx_num = 0
            # specific
            self_suppfx = row[7].strip().split(',')[1:]
            other_suppfx = row[8].strip().split(',')[1:]
            self_subpfx = row[9].strip().split(',')[1:]
            other_subpfx = row[10].strip().split(',')[1:]
            for item in self_suppfx:
                if item.split(' ')[-1] == '0' or item.split(' ')[-1] == '1':
                    self_valid_suppfx_num += 1
                self_suppfx_num += 1
            for item in self_subpfx:
                if item.split(' ')[-1] == '0' or item.split(' ')[-1] == '1':
                    self_valid_subpfx_num += 1
                self_subpfx_num += 1
            for item in other_suppfx:
                if item.split(' ')[-1] == '0' or item.split(' ')[-1] == '1':
                    other_valid_suppfx_num += 1
                other_suppfx_num += 1
            for item in other_subpfx:
                if item.split(' ')[-1] == '0' or item.split(' ')[-1] == '1':
                    other_valid_subpfx_num += 1
                other_subpfx_num += 1
            
            # asn_roa_change = 0
            # if int(row[1]) in asn_roa_activity.keys():
            #     asn_roa_change = asn_roa_activity[int(row[1])]
            
            # asn_moas_num = 0
            # if int(row[1]) in asn_moas_activity.keys():
            #     asn_moas_num = len(asn_moas_activity[int(row[1])])
            
            cover_asn = len(roa.cover_pfx(row[0]))
            
            # Tag
            tag = -1
            if row[33] == 'Valid':
                # Positive
                tag = 0
            elif row[32] != '':
                # Negative
                tag = 1
            
            # Remove Begin After 20
            if tag != 1 and event_lastDay[event][1] >= 20:
                continue
            # Remove Same Org in Negative
            # if tag == 1 and same_org:
            #     continue
            
            attributes = [
                row[0], row[1],
                last_days, int(rel_self), int(rel_p2c), int(rel_p2p), int(rel_c2p), 
                int(same_org), vp_num, invalid_type, vrp_num,
                vp_received_num, 
                self_subpfx_num, self_suppfx_num, other_subpfx_num, other_suppfx_num, 
                self_valid_subpfx_num, self_valid_suppfx_num, other_valid_subpfx_num, 
                other_valid_suppfx_num, 
                vrp_as_num, 
                # asn_roa_change, asn_moas_num, 
                cover_asn
            ]
            date_event[date].append(attributes)

            if tag != -1:
                if tag == 0 and (date == ('2020', '07') or date == ('2020', '11')):
                    continue
                x.append(attributes[2:])

                # if tag == 1:
                # attributes.append(tag)
                # attributes.insert(0, '{}-{}'.format(date[0], date[1]))
                # write_path = os.path.join(DATASET_DIR, 'train_set.csv')
                # write_file = open(write_path, 'a')
                # writer = csv.writer(write_file)
                # writer.writerow(attributes)
                # write_file.close()

                if tag == 1 and same_org:
                    print('Get')

                y.append(tag)

        data_file.close()


def select_attributes(row):
    res = []
    # Last Days: row[0]
    if 'last_days' in SELECT_ATTRIBUTES:
        res.append(int(row[0]))
    # Business Relationships: row[1:5]
    if 'rel' in SELECT_ATTRIBUTES:
        for elem in row[1:5]:
            res.append(int(elem))
    # Organization: row[5]
    if 'org' in SELECT_ATTRIBUTES:
        res.append(int(row[5]))
    # Visibility VP Num: row[6]
    if 'vp_num' in SELECT_ATTRIBUTES:
        res.append(int(row[6]))
    # Invalid Type: row[7]
    if 'invalid_type' in SELECT_ATTRIBUTES:
        res.append(int(row[7]))
    # Related VRP_NUM: row[8]
    if 'vrp_num' in SELECT_ATTRIBUTES:
        res.append(int(row[8]))
    # VP Received Num (Whether already in Routing Table): row[9]
    if 'vp_received_num' in SELECT_ATTRIBUTES:
        res.append(int(row[9]))
    # Related Prefix Num: row[10:14]
    if 'self_pfx_num' in SELECT_ATTRIBUTES:
        for elem in row[10:12]:
            res.append(int(elem))
    if 'other_pfx_num' in SELECT_ATTRIBUTES:
        for elem in row[12:14]:
            res.append(int(elem))
    # Related Valid/Unknown Prefix Num: row[14:18]
    if 'self_valid_pfx_num' in SELECT_ATTRIBUTES:
        for elem in row[14:16]:
            res.append(int(elem))
    if 'other_valid_pfx_num' in SELECT_ATTRIBUTES:
        for elem in row[16:18]:
            res.append(int(elem))
    if 'vrp_as_num' in SELECT_ATTRIBUTES:
        res.append(int(row[18]))
    if 'cover_asn' in SELECT_ATTRIBUTES:
        res.append(int(row[19]))
    return res


def random_select_sample(train_pos_index, pos_visited_index, select_num):
    sample_index = set()
    while len(sample_index) < select_num:
        rand_index = int(random.random() * len(x))
        if rand_index in train_pos_index and rand_index not in pos_visited_index:
            sample_index.add(rand_index)
            pos_visited_index.add(rand_index)
    return sample_index


def get_sample_attr_tag(sample_index):
    sample_attribute = []
    sample_tag = []
    for index in sample_index:
        sample_attribute.append(select_attributes(x[index]))
        sample_tag.append(y[index])
    return sample_attribute, sample_tag


def generate_estimators(train_pos_index, train_neg_index):
    estimators = []
    pos_visited_index = set()
    estimator_num = int(len(train_pos_index)/len(train_neg_index)/TRAIN_P_N)
    for i in range(estimator_num):
        sample_index = random_select_sample(train_pos_index, pos_visited_index, len(train_neg_index)*TRAIN_P_N)
        sample_attribute, sample_tag = get_sample_attr_tag(sample_index.union(train_neg_index))

        clf = None
        parameters = None
        if CLASSIFIER == 'decision_tree':
            clf = DecisionTreeClassifier()
            parameters = {
                'criterion': ['gini', 'entropy'],
                'splitter': ['best', 'random'],
                'max_features': ['auto', 'sqrt', 'log2', None],
            }
        elif CLASSIFIER == 'extra_tree':
            clf = ExtraTreesClassifier()
            parameters = {
                'n_estimators': [100, 200, 300],
                'criterion': ['gini', 'entropy'], 
                'max_features': ['auto', 'sqrt', 'log2'],
            }
        grid_search = GridSearchCV(clf, parameters, n_jobs=-1)
        grid_search.fit(sample_attribute, sample_tag)
        estimators.append(grid_search.best_estimator_)
        log('   Estimator {}/{} best_parameter: {}'.format(i+1, estimator_num, grid_search.best_params_))
    return estimators


def _predict(attr, estimators):
    res = []
    for est in estimators:
        res.append(list(est.predict([attr]))[0])
    return max(res, key=res.count)


def predict(attrs, estimators):
    pred_res = []
    for i in range(len(attrs)):
        pred_res.append(_predict(attrs[i], estimators))
    return pred_res


def cross_validation():
    log('Cross Validation.')
    skf = StratifiedKFold(n_splits=KFOLD, shuffle=True, random_state=42)

    for train_index, test_index in skf.split(x, y):
        train_index = set(train_index)
        test_index = set(test_index)

        # Split Train Positive & Train Negative
        train_pos_index = set()
        train_neg_index = set()
        for index in train_index:
            if y[index] == 0:
                train_pos_index.add(index)
            else:
                train_neg_index.add(index)
        log('[POS: {}, NEG: {}]'.format(len(train_pos_index), len(train_neg_index)))

        estimators = generate_estimators(train_pos_index, train_neg_index)

        # Predict
        verify_attribute, verify_tag = get_sample_attr_tag(test_index)
        ros = RandomOverSampler()
        verify_attribute, verify_tag = ros.fit_sample(verify_attribute, verify_tag)
        verify_attribute = list(verify_attribute)
        verify_tag = list(verify_tag)
        log('After Random Over Sampler, Test Pos: {}, Test Neg: {}'.format(verify_tag.count(0), verify_tag.count(1)))

        pred_res = predict(verify_attribute, estimators)
        log('---------- Classification Report ----------')
        log(classification_report(verify_tag, pred_res))
        # Accuracy
        same = 0
        for i in range(len(verify_tag)):
            if verify_tag[i] == pred_res[i]:
                same += 1
        log('Accuracy: {}'.format(float(same) / len(verify_tag)))

        # Write File
        write_file = open(CROSS_VALIDATION_RES_PATH, 'a')
        writer = csv.writer(write_file)
        for i in range(len(verify_tag)):
            writer.writerow([verify_tag[i], pred_res[i]])
        write_file.close()
    
    # Overall Estimator
    verify_tag = []
    pred_res = []
    same = 0
    open_file = open(CROSS_VALIDATION_RES_PATH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        verify_tag.append(int(row[0]))
        pred_res.append(int(row[1]))
        if row[0] == row[1]:
            same += 1
    open_file.close()

    log('---------------------------- Overall ----------------------------')
    log('[Total: {}]'.format(len(pred_res)))
    log(classification_report(verify_tag, pred_res))
    log('Accuracy: {}'.format(float(same) / len(verify_tag)))


def train_modules():
    log('Train Modules.')
    log('Total Samples: {}, Positive: {}, Negative: {}, P/N: {}'.format(len(x), y.count(0), y.count(1), float(y.count(0))/y.count(1)))

    train_index = set([i for i in range(len(y))])
    train_pos_index = set()
    train_neg_index = set()

    for index in train_index:
        if y[index] == 0:
            train_pos_index.add(index)
        else:
            train_neg_index.add(index)
    
    log('[POS: {}, NEG: {}]'.format(len(train_pos_index), len(train_neg_index)))

    estimators = generate_estimators(train_pos_index, train_neg_index)

    # Dump Estimators
    for i in range(len(estimators)):
        module_file = os.path.join(MODULE_PATH, 'module_{}.pickle'.format(i+1))
        open_file = open(module_file, 'wb')
        pickle.dump(estimators[i], open_file)
        open_file.close()
    
    return estimators


def predict_month(estimators):
    log('Predict Month.')
    for date in date_list:
        cnt_hijack = 0
        total_event = 0

        log(date)
        for event in date_event[date]:
            attributes = select_attributes(event[2:])
            pred_res = predict([attributes], estimators)
            event.append(pred_res[0])
            
            if pred_res[0] == 1:
                cnt_hijack += 1
            total_event += 1

            write_file = open(os.path.join(MONTH_PATH, '{}-{}_pred.csv'.format(date[0], date[1])), 'a')
            writer = csv.writer(write_file)
            writer.writerow(event)
            write_file.close()
        log('  total_event: {}, hijack: {}, false_positive: {}, hijack_percent = {}'.format(total_event, cnt_hijack, total_event-cnt_hijack, float(cnt_hijack)/total_event))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_p_n', type=int)
    parser.add_argument('--classifier')
    parser.add_argument('--cross_validation', action='store_true')
    parser.add_argument('--predict_month', action='store_true')
    # Select Attribute
    parser.add_argument('--last_days', action='store_true')
    parser.add_argument('--rel', action='store_true')
    parser.add_argument('--org', action='store_true')
    parser.add_argument('--vp_num', action='store_true')
    parser.add_argument('--invalid_type', action='store_true')
    parser.add_argument('--cover_asn', action='store_true')
    parser.add_argument('--vrp_num', action='store_true')
    parser.add_argument('--vrp_as_num', action='store_true')
    parser.add_argument('--vp_received_num', action='store_true')
    parser.add_argument('--self_pfx_num', action='store_true')
    parser.add_argument('--other_pfx_num', action='store_true')
    parser.add_argument('--self_valid_pfx_num', action='store_true')
    parser.add_argument('--other_valid_pfx_num', action='store_true')
    args = parser.parse_args()

    TRAIN_P_N = args.train_p_n
    CLASSIFIER = args.classifier
    PREDICT_MONTH = args.predict_month
    LOG_PATH = './logs/simulation_{}.log'.format(TRAIN_P_N)
    MONTH_PATH = './dataset/month/simulation_{}'.format(TRAIN_P_N)
    MODULE_PATH = './modules/simulation_{}'.format(TRAIN_P_N)
    CROSS_VALIDATION = args.cross_validation
    CROSS_VALIDATION_RES_PATH = './pred_res/cross_validation_{}.csv'.format(TRAIN_P_N)
    get_select_attributes(args)

    os.system('rm -rf {} {}'.format(MONTH_PATH, MODULE_PATH))
    os.system('mkdir {} {}'.format(MONTH_PATH, MODULE_PATH))
    os.system('rm {}'.format(CROSS_VALIDATION_RES_PATH))

    log(SELECT_ATTRIBUTES)
    log('[TRAIN_P_N={}, CLASSIFIER: {}]'.format(TRAIN_P_N, CLASSIFIER))
    # 1. Load Samples
    load_samples()

    # 2. K Fold Cross Validation
    if CROSS_VALIDATION:
        cross_validation()

    # 3. Train Module with All Samples with Tag
    estimators = train_modules()

    # 4. Predict All Samples
    if PREDICT_MONTH:
        predict_month(estimators)

    log('\n\n\n\n')

