import os
import csv
import _validate


FILE_DIR = './'
ROA_DIR = './roas'


date_list = (
    ('2020', '03'),
    ('2020', '05'),
    ('2020', '07'),
    ('2020', '09'),
    ('2020', '11'),
    ('2021', '01'),
    ('2021', '03'),
    ('2021', '05'),
    ('2021', '07'),
    ('2021', '09')
)
validate_date_list = (
    ('2020', '04', '30'),
    ('2020', '06', '30'),
    ('2020', '08', '31'),
    ('2020', '10', '31'),
    ('2020', '12', '31'),
    ('2021', '02', '28'),
    ('2021', '04', '30'),
    ('2021', '06', '30'),
    ('2021', '08', '31'),
    ('2021', '10', '31')
)


def load_file(file_path, roa_path, i):
    roa = _validate.ROA()
    roa.load_roa(roa_path)

    open_file = open(file_path, 'r')
    reader = csv.reader(open_file)
    for row in reader:
        pfx = row[0]
        ori = row[1]

        status, valid_roa = roa.validate(pfx, ori)
        row.append(status)

        write_file = open('{}-{}_1.csv'.format(date_list[i][0], date_list[i][1]), 'a')
        writer = csv.writer(write_file)
        writer.writerow(row)
        write_file.close()

    open_file.close()


if __name__ == '__main__':
    for i in range(len(date_list)):
        print(date_list[i])
        file_path = os.path.join(FILE_DIR, '{}-{}_v4_vp_lastTime_rel_judge_change_hijack.csv'.format(date_list[i][0], date_list[i][1]))
        roa_path = os.path.join(ROA_DIR, '{}-{}-{}'.format(validate_date_list[i][0], validate_date_list[i][1], validate_date_list[i][2]))

        load_file(file_path, roa_path, i)
        # break




