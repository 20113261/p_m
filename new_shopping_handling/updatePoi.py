# encoding=utf8

import sys
import MySQLdb
import csv
import pymysql
import pandas
from Config.settings import shop_merge_conf

table_name = 'chat_shopping_new'


def insert_db(args):
    conn = pymysql.connect(host='10.10.180.145', user='hourong', charset='utf8', passwd='hourong', db='data_prepare')
    with conn as cursor:
        sql = 'replace into chat_shopping VALUES ({})'.format(','.join(['%s'] * 55))
        res = cursor.executemany(sql, args)
    conn.close()
    return res


def getCandOnlineData(update_cid_file):
    conn = MySQLdb.connect(**shop_merge_conf)
    cursor = conn.cursor()

    get_column_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name='{}'".format(
        table_name)
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
    map_info_idx = 3
    city_idx = 4
    first_img_idx = 35
    source_idx = 38
    norm_tag_idx = 33
    open_time_idx = 20
    test_idx = 54
    online_idx = 53
    address_idx = 5

    column_count = 55
    all_data = []
    report_data = pandas.DataFrame(
        columns=['ID', '城市名', '国家', '开发库中购物个数', 'test 上线购物个数', '上线购物占比', '上线购物有图占比', '开发库中 daodao 购物个数',
                 '上线 daodao 个数', '上线 daodao 购物占比'])
    report_count = 0
    for cand_line in open(update_cid_file):
        line_list = cand_line.strip().split('\t')

        if 4 != len(line_list):
            continue

        line = line_list[0]
        get_sql = "select * from {} where city_id = '{}'".format(table_name, line)

        cursor.execute(get_sql)

        datas = cursor.fetchall()

        cand_data = []

        name_null_fail_count = 0
        city_null_fail_count = 0
        first_img_null_fail_count = 0
        norm_tag_null_fail_count = 0
        map_fail_count = 0

        img_succeed_count = 0
        daodao_count = 0
        daodao_succeed_count = 0

        for data in datas:
            if len(data) < column_count:
                print('table column count is ' + str(len(data)) + '. it should not less than ' + str(column_count))
                sys.exit(1)

            word_list = []

            for idx in range(len(data)):
                if isinstance(data[idx], str):
                    word_list.append(data[idx].strip().replace('\t', '').replace('\n', ''))
                elif data[idx] is None:
                    word_list.append('NULL')
                elif isinstance(data[idx], int) or isinstance(data[idx], float):
                    word_list.append(str(data[idx]))
                else:
                    word_list.append('')

            # 置位 right_data
            right_data = True

            # daodao 统计部分
            if 'daodao' in word_list[source_idx]:
                daodao_count += 1

            # 数据检查部分

            # name
            if word_list[name_idx].lower() in ('', 'null', '0') and word_list[name_en_idx].lower() in ('', 'null', '0'):
                name_null_fail_count += 1
                right_data = False

            # city
            if word_list[city_idx].lower() in ('', 'null', '0'):
                city_null_fail_count += 1
                right_data = False

            # map_info
            try:
                lat = float(word_list[map_info_idx].strip().split(',')[0])
                lgt = float(word_list[map_info_idx].strip().split(',')[1])
            except:
                map_fail_count += 1
                right_data = False

            if 'daodao' in word_list[source_idx]:
                daodao_succeed_count += 1

            # 数据更改部分
            # name
            if word_list[name_idx].lower() in ('', 'null', '0'):
                word_list[name_idx] = word_list[name_en_idx]

            # name_en
            if word_list[name_en_idx].lower() in ('', 'null', '0'):
                word_list[name_en_idx] = word_list[name_idx]

            # address
            if word_list[address_idx].lower() in ('null', '0'):
                word_list[address_idx] = ''

            # open time
            if word_list[open_time_idx].lower() in ('', 'null', '0'):
                word_list[open_time_idx] = '<*><*><08:00-20:00><SURE>'

            word_list[online_idx] = 'Open'
            word_list[test_idx] = 'Open'

            if right_data:
                cand_data.append(word_list)

        if len(cand_data) > 0:
            csv_writer.writerows(cand_data)
            all_data.extend(cand_data)

        city_id, city_name, city_name_en, country = cand_line.strip().split('\t')
        report_data.loc[report_count] = [
            city_id,
            city_name,
            country,
            len(datas),
            len(cand_data),
            "{0:04f}%".format(100 * len(cand_data) / float(len(datas))) if len(datas) != 0 else '无穷大',
            "{0:04f}%".format(100 * img_succeed_count / float(len(cand_data))) if len(cand_data) != 0 else '无穷大',
            daodao_count,
            daodao_succeed_count,
            "{0:04f}%".format(
                100 * daodao_succeed_count / float(daodao_count)) if daodao_count != 0 else '无穷大'
        ]
        report_count += 1
        print(cand_line.strip() + '\tall:' + str(len(datas)) + "\tget:" + str(len(cand_data)) + '\tname_null:' + str(
            name_null_fail_count) + '\tcity_null:' + str(city_null_fail_count) + '\timg_null:' + str(
            first_img_null_fail_count) + '\tmap:' + str(map_fail_count) + '\tnorm_tag:' + str(norm_tag_null_fail_count))

    print('Total:', insert_db(all_data))
    out_file.close()
    pandas.DataFrame(report_data).to_csv('report.csv')
    pandas.DataFrame(report_data).to_excel('report.xlsx')


if __name__ == '__main__':
    # if len(sys.argv) != 2:
    #     print('USAG:python a.py cid_file[one cid each line]')
    #     sys.exit(1)
    #
    # cid_file = sys.argv[1]

    cid_file = 'cid_file'
    getCandOnlineData(cid_file)
