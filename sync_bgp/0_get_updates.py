import os
import argparse
import datetime
import threading
import multiprocessing
from time import sleep


routeviews = ['','route-views3/','route-views4/','route-views6/','route-views.amsix/','route-views.chicago/','route-views.chile/','route-views.eqix/','route-views.flix/','route-views.gorex/','route-views.isc/','route-views.kixp/','route-views.jinx/','route-views.linx/','route-views.napafrica/','route-views.nwax/','route-views.phoix/','route-views.telxatl/','route-views.wide/','route-views.sydney/','route-views.saopaulo/','route-views2.saopaulo/','route-views.sg/','route-views.perth/','route-views.sfmix/','route-views.soxrs/','route-views.mwix/','route-views.rio/','route-views.fortaleza/','route-views.gixa/','route-views.bdix/']
ris = ['rrc00','rrc01','rrc02','rrc03','rrc04','rrc05','rrc06','rrc07','rrc08','rrc09','rrc10','rrc11','rrc12','rrc13','rrc14','rrc15','rrc16','rrc18','rrc19','rrc20','rrc21','rrc22','rrc23','rrc24','rrc25', 'rrc26']
basic_dir = './'
basic_dir2 = '../'
log_path = os.path.join(basic_dir, 'sync.log')
n_thread = 30


def print_to_log(message, path=log_path):
    output_log = open(path,'a')
    print('[{}]'.format(datetime.datetime.now()) + message,file=output_log)
    output_log.close()


def getStrDate(collect_time):
    return datetime.datetime.strftime(collect_time, '%Y-%m-%d')


def getSplitDate(collectTime):
    strTime = str(collectTime)
    date_time = strTime.split(' ')
    date = date_time[0].split('-')
    time = date_time[1].split(':')
    return date[0],date[1],date[2],time[0],time[1],time[2]


def downloadFile(url,get_file_name,reverse_file_name, write_dir, cnt, total_num):
    print_to_log('({}/{})'.format(cnt,total_num))
    os.chdir(write_dir)
    os.system('wget ' + url + ' -O ./' + get_file_name)
    if(os.stat(get_file_name).st_size != 0):
        os.system('bgpdump -m ' + get_file_name + ' > ' + reverse_file_name)
    os.system('rm ' + get_file_name)
    print_to_log(reverse_file_name + 'done.')


def sync_update(static_start_time, record_from, pool):
    write_dir = basic_dir2 + 'tempUpdates/' + getStrDate(static_start_time) + '/'
    year, month, day, hour, minute, second = getSplitDate(static_start_time)
    if(record_from == 'routeviews'):
        for i in range(len(routeviews)):
            origin_file_name = 'updates.' + year + month + day + '.' + hour + minute + '.bz2'
            reverse_file_name = ''
            get_file_name = ''
            if(routeviews[i] == ''):
                get_file_name = 'routeviews-updates.' + year + month + day + '.' + hour + minute + '.bz2'
                reverse_file_name = 'routeviews-route-views-' + hour + ':' + minute + ':' + second + '.txt'
            else:
                get_file_name = routeviews[i][:-1] + '-updates.' + year + month + day + '.' + hour + minute + '.bz2'
                reverse_file_name = 'routeviews-' + routeviews[i][:-1] + '-' + hour + ':' + minute + ':' + second + '.txt'
            url = 'http://archive.routeviews.org/' + routeviews[i] + 'bgpdata/' + year + '.' + month + '/UPDATES/' + origin_file_name

            pool.apply_async(downloadFile, args=(url, get_file_name, reverse_file_name, write_dir, i, str(len(routeviews))))
    else:
        for i in range(len(ris)):
            origin_file_name = 'updates.' + year + month + day + '.' + hour + minute + '.gz'
            get_file_name = ris[i] + '-updates.' + year + month + day + '.' + hour + minute + '.gz'
            # http://data.ris.ripe.net/rrc00/2021.05/updates.20210501.0000.gz
            # http://data.ris.ripe.net/rrc01/2021.05/updates.20210523.0215.gz
            url = 'http://data.ris.ripe.net/' + ris[i] + '/' + year + '.' + month + '/' + origin_file_name
            reverse_file_name = 'ris-' + ris[i] + '-' + hour + ':' + minute + ':' + second + '.txt'
            
            pool.apply_async(downloadFile, args=(url, get_file_name, reverse_file_name, write_dir, i, str(len(ris))))



def sync_rib(static_start_time, pool):
    write_dir = basic_dir2 + 'tempRibs/' + getStrDate(static_start_time) + '/'
    year, month, day, hour, minute, second = getSplitDate(static_start_time)
    for i in range(len(routeviews)):
        origin_file_name = 'rib.' + year + month + day + '.' + hour + minute + '.bz2'
        get_file_name = ''
        reverse_file_name = ''
        if(routeviews[i] == ''):
            get_file_name = 'routeviews-rib.' + year + month + day + '.' + hour + minute + '.bz2'
            reverse_file_name = 'routeviews-route-views-' + hour + ':' + minute + ':' + second + '.txt'
        else:
            get_file_name = routeviews[i][:-1] + '-rib.' + year + month + day + '.' + hour + minute + '.bz2'
            reverse_file_name = 'routeviews-' + routeviews[i][:-1] + '-' + hour + ':' + minute + ':' + second + '.txt'
        url = 'http://archive.routeviews.org/' + routeviews[i] + 'bgpdata/' + year + '.' + month + '/RIBS/' + origin_file_name

        pool.apply_async(downloadFile, args=(url, get_file_name, reverse_file_name, write_dir, str(i), str(len(routeviews))))

    for i in range(len(ris)):
        origin_file_name = 'bview.' + year + month + day + '.' + hour + minute + '.gz'
        get_file_name = ris[i] + '-bview.' + year + month + day + '.' + hour + minute + '.gz'
        # http://data.ris.ripe.net/rrc00/2021.05/bview.20210520.0000.gz
        url = 'http://data.ris.ripe.net/' + ris[i] + '/' + year + '.' + month + '/' + origin_file_name
        reverse_file_name = 'ris-' + ris[i] + '-' + hour + ':' + minute + ':' + second + '.txt'

        pool.apply_async(downloadFile, args=(url, get_file_name, reverse_file_name, write_dir, str(i), str(len(ris))))


def sync(args, static_start_time, end_time):
    global n_thread
    pool = multiprocessing.Pool(n_thread)
    while(True):
        if(static_start_time >= end_time):
            break
        if(args.updates):
            os.chdir(basic_dir2 + 'tempUpdates/' + getStrDate(static_start_time) + '/')
            # print_to_log('Updates...')
            if(static_start_time.minute % 15 == 0):
                sync_update(static_start_time, 'routeviews', pool)
            sync_update(static_start_time, 'ris', pool)
        
        if(args.ribs and static_start_time.hour == 0 and static_start_time.minute == 0):
            os.chdir(basic_dir2 + 'tempRibs/' + getStrDate(static_start_time) + '/')
            # print_to_log('Ribs...')
            sync_rib(static_start_time, pool)
        
        static_start_time = static_start_time + datetime.timedelta(minutes=5)
    
    pool.close()
    pool.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start',help='%%Y-%%m-%%d-%%H:%%M:%%S')
    parser.add_argument('end',help='%%Y-%%m-%%d-%%H:%%M:%%S')
    parser.add_argument('-r','--ribs',help='collect RIBs in all collectors',action='store_true')
    parser.add_argument('-u','--updates',help='collect Updates in all collectors',action='store_true')
    # parser.add_argument('-t','--thread',help='set the number of threads',type=int)
    args = parser.parse_args()

    start_time = datetime.datetime.strptime(args.start,'%Y-%m-%d-%H:%M:%S')
    end_time = datetime.datetime.strptime(args.end,'%Y-%m-%d-%H:%M:%S')

    while True:

        if(not os.path.exists(os.path.join(basic_dir2, 'tempUpdates', getStrDate(start_time)))):
            os.chdir(basic_dir2 + 'tempUpdates/')
            os.mkdir(getStrDate(start_time))
        if(not os.path.exists(os.path.join(basic_dir2, 'tempRibs', getStrDate(start_time)))):
            os.chdir(basic_dir2 + 'tempRibs/')
            os.mkdir(getStrDate(start_time))
        
        if(start_time > end_time):
            break
        
        print_to_log('Sync ' + str(start_time))

        # last_two_day = start_time - datetime.timedelta(days=2)
        # last_two_day_standard = getStrDate(last_two_day)
        # while(os.path.exists(basic_dir2 + 'tempUpdates/' + last_two_day_standard + '/') or os.path.exists(basic_dir2 + 'tempRibs/' + last_two_day_standard + '/')):
        #     print_to_log('sleep 10s')
        #     sleep(10)

        sync(args, start_time, start_time + datetime.timedelta(days=1))

        start_time = start_time + datetime.timedelta(days=1)
