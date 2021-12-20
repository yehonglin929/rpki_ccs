import os
import datetime


date_list = (
    ('2020-02-01', '2020-03-01'),
    ('2020-04-01', '2020-05-01'),
    ('2020-06-01', '2020-07-01'),
    ('2020-08-01', '2020-09-01'),
    ('2020-10-01', '2020-11-01'),
    ('2020-12-01', '2021-01-01'),
    ('2021-02-01', '2021-03-01'),
    ('2021-04-01', '2021-05-01'),
    ('2021-06-01', '2021-07-01'),
    ('2021-08-01', '2021-09-01')
)


def log(message):
    output_log = open('./0_batch_sync_roas.log', 'a')
    print(message, file=output_log)
    output_log.close()


def get_between_date(begin_date, end_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    while begin_date < end_date:
        date_list.append(datetime.datetime.strftime(begin_date, '%Y-%m-%d'))
        begin_date += datetime.timedelta(days=1)
    return date_list


def sync_roa(sync_date):
    os.system('python3 0_get_roas.py --date={}'.format(sync_date))


if __name__ == '__main__':
    for date in date_list:
        sync_list = get_between_date(date[0], date[1])
        for sync_date in sync_list:
            log(sync_date)
            sync_roa(sync_date)
    
    
