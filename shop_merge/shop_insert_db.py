# coding=utf-8

import json
import pymysql
import toolbox.Common
from pymysql.cursors import DictCursor
from collections import defaultdict

others_name_list = ['source']

json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts', 'tagid',
                  'introduction']
norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone',
                  'opentime', 'prize', 'traveler_choice', 'imgurl']

key_priority = defaultdict(int)
key_priority.update({
    'daodao': 10,
    'qyer': 9,
    'yelp': 8,
    'tripadvisor': 10
})


def get_key_by_priority(src: dict):
    if len(src.keys()) == 0:
        return None
    for k, v in sorted(src.items(), key=lambda x: key_priority[x[0]], reverse=True):
        if toolbox.Common.is_legal(v):
            return v
    return None


def get_shop_id_list():
    shop_id_list = []
    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='shop_merge')
    cursor = conn.cursor()
    sql = 'select distinct source_id from shop_unid'
    cursor.execute(sql)
    for line in cursor.fetchall():
        shop_id_list.append(line[0])
    return shop_id_list


def get_shop_dict(shop_id_list):
    shop_dict = defaultdict(dict)
    # conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', password='mioji1109', charset='utf8',
    #                        db='shop_merge')
    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8',
                           db='shop_merge')
    cursor = conn.cursor(cursor=DictCursor)
    sql = "select * from shop where id in (%s)" % ','.join(
        ["\"" + x + "\"" for x in shop_id_list])
    cursor.execute(sql)
    for line in cursor.fetchall():
        source_id = line['id']
        source = line['source']
        shop_dict[(source, source_id)] = line
    cursor.close()
    return shop_dict


def get_task():
    shop_id_list = get_shop_id_list()
    shop_dict = get_shop_dict(shop_id_list)

    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='shop_merge')
    cursor = conn.cursor(cursor=DictCursor)
    # sql = "select id, source, group_concat(source_id) as source_id, from shop_unid GROUP BY id"
    sql = '''SELECT
  id,
  city_id,
  group_concat(concat(source, '|', source_id) SEPARATOR '|_||_|') AS union_info
FROM shop_unid
GROUP BY id'''

    cursor.execute(sql)
    count = 0
    data = []

    for line in cursor.fetchall():
        miaoji_id = line['id']
        city_id = line['city_id']
        union_info = line['union_info']

        data_dict = defaultdict(dict)
        can_be_used = False

        # 初始化融合信息
        for each_name in (json_name_list + norm_name_list + others_name_list):
            data_dict[each_name] = {}

        for s_sid in union_info.split('|_||_|'):
            source, source_id = s_sid.split('|')
            if not source_id or not source:
                continue

            shop_info = shop_dict[(source, source_id)]
            if shop_info == {}:
                continue

            can_be_used = True

            for each_name in (json_name_list + norm_name_list + others_name_list):
                if str(shop_dict[each_name]).upper() not in ('NULL', ''):
                    data_dict[each_name][source] = shop_info[each_name]

        if not can_be_used:
            print('union_info', union_info)
            continue
        new_data_dict = {}
        for norm_name in norm_name_list:
            new_data_dict[norm_name] = get_key_by_priority(data_dict[norm_name]) or ''

        for json_name in json_name_list:
            new_data_dict[json_name] = json.dumps(data_dict[json_name])

        new_data_dict['phone'] = new_data_dict['phone'].replace('电话号码：', '').strip()

        # 添加 source
        source = '|'.join(sorted(data_dict['source'].values()))
        data_dict = new_data_dict

        sql = 'insert ignore into ' \
              'chat_shopping(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
              '`map_info`,`address`,`star`,`plantocount`,`beentocount`,' \
              '`real_ranking`,`grade`,`commentcount`,`tagid`,`url`,`website_url`,' \
              '`phone`,`introduction`,`open_desc`,`recommend_lv`,`prize`,' \
              '`traveler_choice`,`image`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s)'

        data.append((
            miaoji_id, data_dict['name'], data_dict['name_en'], source, city_id,
            data_dict['map_info'], data_dict['address'],
            data_dict['star'], data_dict['plantocounts'], data_dict['beentocounts'],
            data_dict['ranking'], data_dict['grade'],
            data_dict['commentcounts'],
            data_dict['tagid'], data_dict['url'], data_dict['site'], data_dict['phone'],
            data_dict['introduction'],
            data_dict['opentime'], data_dict['recommend_lv'], data_dict['prize'],
            data_dict['traveler_choice'],
            data_dict['imgurl']))

        if count % 1000 == 0:
            print("Total:", count)
            cursor = conn.cursor()
            print("Insert:", cursor.executemany(sql, data))
            cursor.close()
            data = []
        count += 1
    print("Total:", count)
    cursor = conn.cursor()
    print("Insert:", cursor.executemany(sql, data))
    cursor.close()


if __name__ == '__main__':
    get_task()
