# encoding=utf8

import sys
import MySQLdb
import csv
import pymysql

test_db_ip = '10.10.114.35'
test_db_user = 'reader'
test_db_passwd = 'miaoji1109'
db_name = 'rest_merge'
table_name = 'chat_restaurant'


def insert_db(args):
    conn = pymysql.connect(host='localhost', user='hourong', charset='utf8', passwd='hourong', db='data_prepare')
    with conn as cursor:
        sql = 'replace into chat_restaurant VALUES ({})'.format(','.join(['%s'] * 67))
        res = cursor.executemany(sql, args)
    conn.close()
    return res


def getCandOnlineData(update_cid_file):
    conn = MySQLdb.connect(host=test_db_ip, user=test_db_user, charset='utf8', passwd=test_db_passwd, db=db_name)
    cursor = conn.cursor()

    get_column_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name='" + table_name + "'";
    cursor.execute(get_column_sql)

    datas = cursor.fetchall()

    column_list = []

    for data in datas:
        column_list.append(data[0])

    out_file = open(table_name + '.csv', 'w')
    csv_writer = csv.writer(out_file)

    csv_writer.writerow(column_list)

    name_idx = 1
    name_en_idx = 2
    map_info_idx = 7
    city_idx = 6
    first_img_idx = 46
    open_time_idx = 26
    online_idx = 45

    column_count = 64
    all_data = []
    for cand_line in open(update_cid_file):
        line_list = cand_line.strip().split('\t')

        if 3 != len(line_list):
            continue

        line = line_list[0]
        get_sql = "select * from " + table_name + " where city_id = '" + line + "'"

        cursor.execute(get_sql)

        datas = cursor.fetchall()

        cand_data = []

        name_null_fail_count = 0
        city_null_fail_count = 0
        first_img_null_fail_count = 0
        map_fail_count = 0

        for data in datas:
            if len(data) < column_count:
                print('table column count is ' + str(len(data)) + '. it should not less than ' + str(column_count))
                sys.exit(1)

            word_list = []

            for idx in range(len(data)):
                if isinstance(data[idx], str):
                    word_list.append(data[idx].strip().replace('\t', '').replace('\n', ''))
                elif None == data[idx]:
                    word_list.append('NULL')
                elif isinstance(data[idx], int) or isinstance(data[idx], float):
                    word_list.append(str(data[idx]))
                else:
                    word_list.append('')

            is_legel_data = True

            if (word_list[name_idx].lower() in ('', 'null') and word_list[name_en_idx].lower() in ('', 'null')):
                name_null_fail_count += 1
                is_legel_data = False

            if is_legel_data and word_list[city_idx].lower() in ('', 'null'):
                city_null_fail_count += 1
                is_legel_data = False

            if is_legel_data and word_list[first_img_idx].lower() in ('', 'null'):
                first_img_null_fail_count += 1
                is_legel_data = False

            if is_legel_data:
                try:
                    lat = float(word_list[map_info_idx].strip().split(',')[0])
                    lgt = float(word_list[map_info_idx].strip().split(',')[1])
                except:
                    map_fail_count += 1
                    is_legel_data = False

            if word_list[name_idx].lower() in ('', 'null'):
                word_list[name_idx] = word_list[name_en_idx]
            elif word_list[name_en_idx].lower() in ('', 'null'):
                word_list[name_en_idx] = word_list[name_idx]

            if word_list[open_time_idx].lower() in ('', 'null'):
                word_list[open_time_idx] = '<*><*><00:00-23:55><SURE>'

            if is_legel_data:
                word_list[online_idx] = 1
            else:
                word_list[online_idx] = 0

            cand_data.append(word_list)

        if len(cand_data) > 0:
            csv_writer.writerows(cand_data)
            all_data.extend(cand_data)

        print(cand_line.strip() + '\tall:' + str(len(datas)) + "\tget:" + str(len(cand_data)) + '\tname_null:' + str(
            name_null_fail_count) + '\tcity_null:' + str(city_null_fail_count) + '\timg_null:' + str(
            first_img_null_fail_count) + '\tmap:' + str(map_fail_count))

    print('Total:', insert_db(all_data))
    out_file.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAG:python a.py cid_file[one cid each line]')
        sys.exit(1)

    cid_file = sys.argv[1]

    getCandOnlineData(cid_file)
