# encoding=utf8

import sys
import MySQLdb
import csv
import pymysql
import pandas

test_db_ip = '10.10.114.35'
test_db_user = 'reader'
test_db_passwd = 'miaoji1109'
db_name = 'attr_merge'
table_name = 'chat_attraction'

__online_sql_dict = {
    'host': '10.10.222.209',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

'''
2. 检查open字段。如为空，则统一填为：<*><*><00:00-23:55><SURE>
3.检查address字段。可为空，但内容不能为0
4.online字段仍保持为0，不做更改
'''


def get_online_0_attr(city_id):
    _set = set()
    conn = pymysql.connect(**__online_sql_dict)
    with conn as cursor:
        cursor.execute('select id from chat_attraction where city_id=%s and online!=1', (city_id,))
        for line in cursor.fetchall():
            _set.add(line[0])
    conn.close()
    return _set


def insert_db(args):
    conn = pymysql.connect(host='localhost', user='hourong', charset='utf8', passwd='hourong', db='data_prepare')
    with conn as cursor:
        sql = 'replace into chat_attraction VALUES ({})'.format(','.join(['%s'] * 67))
        res = cursor.executemany(sql, args)
    conn.close()
    return res


def getCandOnlineData(update_cid_file):
    conn = MySQLdb.connect(host=test_db_ip, user=test_db_user, charset='utf8', passwd=test_db_passwd, db=db_name)
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
    name_en_idx = 3
    map_info_idx = 5
    city_idx = 6
    first_img_idx = 40
    source_idx = 8
    norm_tag_idx = 19
    open_time_idx = 28
    online_idx = 42
    address_idx = 21

    column_count = 67
    all_data = []
    report_data = pandas.DataFrame(
        columns=['ID', '城市名', '国家', '开发库中景点个数', 'test 上线景点个数', '上线景点占比', '上线景点有图占比', '开发库中 daodao 景点个数',
                 '上线 daodao 个数', '上线 daodao 景点占比'])
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

        available_poi = get_online_0_attr(line)

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

            # daodao 统计部分
            if 'daodao' in word_list[source_idx]:
                daodao_count += 1

            # 数据筛选部分

            # id check
            if word_list[0] not in available_poi:
                continue

            # 数据检查部分

            # name
            if word_list[name_idx].lower() in ('', 'null', '0') and word_list[name_en_idx].lower() in ('', 'null', '0'):
                name_null_fail_count += 1
                continue

            # city
            if word_list[city_idx].lower() in ('', 'null', '0'):
                city_null_fail_count += 1
                continue

            # norm_tag
            # if word_list[norm_tag_idx].lower() in ('', 'null', '0'):
            #     norm_tag_null_fail_count += 1
            #     continue

            # map_info
            # try:
            #     lat = float(word_list[map_info_idx].strip().split(',')[0])
            #     lgt = float(word_list[map_info_idx].strip().split(',')[1])
            # except:
            #     map_fail_count += 1
            #     continue

            # first_img
            # if word_list[first_img_idx].lower() in ('', 'null', '0'):
            #     if 'daodao' not in word_list[source_idx]:
            #         first_img_null_fail_count += 1
            #         continue
            # else:
            #     img_succeed_count += 1

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
                word_list[open_time_idx] = '<*><*><00:00-23:55><SURE>'

            # 取消 online 为 1 的设置
            # word_list[online_idx] = 1

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
            "{0:04f}%".format(100 * img_succeed_count / float(len(datas))) if len(datas) != 0 else '无穷大',
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
    pandas.DataFrame(report_data).to_excel('report.xlsx')
    pandas.DataFrame(report_data).to_csv('report.csv')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAG:python a.py cid_file[one cid each line]')
        sys.exit(1)

    cid_file = sys.argv[1]

    getCandOnlineData(cid_file)
