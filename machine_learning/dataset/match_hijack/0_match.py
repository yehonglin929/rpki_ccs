import os
import csv


GROUND_TRUTH = '/home/yhl/Code/machine_learning/dataset/match_hijack/groundtruth_hijack_RPKI.csv'
SOURCE_PATH = '/home/yhl/Code/machine_learning/dataset/match_hijack'
TARGET_PATH = '/home/yhl/Code/machine_learning/dataset'


date_list = (
    ('2020', '03'),
    ('2020', '07'),
    ('2020', '11'),
    ('2021', '03'),
    ('2021', '07'),
    ('2021', '09')
)
hijack_event = {
    '2020-03': set(),
    '2020-07': set(),
    '2020-11': set(),
    '2021-03': set(),
    '2021-07': set(),
    '2021-09': set()
}


def standard_date(date):
    year, month, day = date.split('/')
    if len(month) == 1:
        month = '0' + month
    return '{}-{}'.format(year, month)


def load_ground_truth():
    open_file = open(GROUND_TRUTH, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        # DATE, HIJACK_AS, VICTIM_AS, HIJACK_PFX, VICTIM_PFX
        date = standard_date(row[0])
        if date in hijack_event.keys():
            hijack_event[date].add('{},AS{}'.format(row[3], row[1]))
    open_file.close()


def load_file(date):
    source_file = os.path.join(SOURCE_PATH, '{}-{}_v4_vp_lastTime_rel.csv'.format(date[0], date[1]))
    target_file = os.path.join(TARGET_PATH, '{}-{}_v4_vp_lastTime_rel_judge_change_hijack.csv'.format(date[0], date[1]))
    open_file = open(source_file, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        key = '{},AS{}'.format(row[0], row[1])
        if key in hijack_event['{}-{}'.format(date[0], date[1])]:
            # print('match')
            row.append(key)
        else:
            row.append('')
        
        write_file = open(target_file, 'a')
        writer = csv.writer(write_file)
        writer.writerow(row)
        write_file.close()
    open_file.close()


if __name__ == '__main__':
    load_ground_truth()
    for date in date_list:
        print(date)
        load_file(date)
        # break


