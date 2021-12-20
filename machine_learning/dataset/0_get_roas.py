import os
import csv
import argparse


TARGET_PATH = './roas'
TALS = [
    'afrinic.tal',
    'apnic-afrinic.tal',
    'apnic-arin.tal',
    'apnic-iana.tal',
    'apnic-lacnic.tal',
    'apnic-ripe.tal',
    'apnic.tal',
    'arin.tal',
    'lacnic.tal',
    'ripencc.tal'
]


def sync_roa(year, month, date):
    os.chdir(TARGET_PATH)
    if os.path.exists(os.path.join(TARGET_PATH, '{}-{}-{}'.format(year, month, date))):
        os.system('rm -rf {}-{}-{}'.format(year, month, date))
    os.mkdir('{}-{}-{}'.format(year, month, date))
    os.chdir(os.path.join(TARGET_PATH, '{}-{}-{}'.format(year, month, date)))

    for tal in TALS:
        url = 'https://ftp.ripe.net/rpki/{}/{}/{}/{}/roas.csv'.format(tal, year, month, date)
        os.system('wget {}'.format(url))

        if not os.path.exists(os.path.join(TARGET_PATH, '{}-{}-{}'.format(year, month, date), 'roas.csv')):
            os.system('touch roas.csv')
        
        os.system('mv roas.csv {}.csv'.format(tal))
    
        write_file = open('temp.csv', 'a')
        writer = csv.writer(write_file)
        open_file = open('{}.csv'.format(tal), 'r')
        reader = csv.reader(open_file)
        cnt_row = 0
        for row in reader:
            cnt_row += 1
            if cnt_row == 1:
                continue
            writer.writerow(row)
        open_file.close()
        write_file.close()

        os.system('rm {}.csv'.format(tal))
    os.system('mv temp.csv roas.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help='%Y-%m-%d')
    args = parser.parse_args()

    year, month, date = args.date.split('-')

    sync_roa(year, month, date)

