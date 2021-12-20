import os
import csv
import datetime
import argparse


#tals = ['afrinic.tal', 'apnic-afrinic.tal','apnic-arin.tal','apnic-iana.tal','apnic-lacnic.tal','apnic-ripe.tal', 'arin.tal', 'lacnic.tal', 'ripencc.tal']
tals = ['afrinic.tal','apnic.tal', 'arin.tal', 'lacnic.tal', 'ripencc.tal']
basic_dir2 = '../roas/'


def print_to_log(message, date, path='get_roa.log'):
    output_log = open(path, 'a')
    print('[{}] [{}] {}'.format(datetime.datetime.now(), date, message), file=output_log)
    output_log.close()


def sync_roa(tal, date_time):
    os.chdir(os.path.join(basic_dir2, date_time))
    url = 'https://ftp.ripe.net/rpki/{}/{}/{}/{}/roas.csv'.format(tal, *date_time.split('-'))
    os.system('wget {}'.format(url))
    os.system('mv roas.csv {}.csv'.format(tal))


def merge_roa(date_time):
    os.chdir(os.path.join(basic_dir2, date_time))
    file_list = os.walk('.')
    writer = csv.writer(open('roas.csv','a',newline=''))
    for tal in tals:
        reader = csv.reader(open('{}.csv'.format(tal), 'r'))
        cnt_row = 0
        for row in reader:
            cnt_row += 1
            if(cnt_row == 1):
                continue
            writer.writerow(row)
        os.system('rm {}.csv'.format(tal))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start', help='%Y-%m-%d')
    parser.add_argument('end', help='%Y-%m-%d')
    args = parser.parse_args()

    start_time = datetime.datetime.strptime(args.start, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(args.end, '%Y-%m-%d')

    while(start_time <= end_time):
        date_time = datetime.datetime.strftime(start_time, '%Y-%m-%d')
        print_to_log('Start sync roa {}'.format(date_time), date_time)

        if(not os.path.exists(os.path.join(basic_dir2, date_time))):
            os.chdir(basic_dir2)
            os.mkdir(date_time)

        # https://ftp.ripe.net/rpki/afrinic.tal/2021/05/01/roas.csv
        for tal in tals:
            sync_roa(tal, date_time)
        
        merge_roa(date_time)
        print_to_log('Merge done {}'.format(date_time), date_time)

        start_time += datetime.timedelta(days=1)

