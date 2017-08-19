#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/16 上午10:14
# @Author  : Hou Rong
# @Site    : 
# @File    : attr_insert_db.py
# @Software: PyCharm
import toolbox.Common
import pymysql
import json
from pymysql.cursors import DictCursor
from collections import defaultdict

other_name_list = ['source']

json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts', 'tagid',
                  'introduction']

norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone', 'opentime',
                  'prize',
                  'traveler_choice', 'imgurl']

# qyer_list = ['name', 'name_en']
# daodao_list = ['map_info', 'address', 'grade', 'website_url', 'phone', 'open_desc', 'visit_time',
#                'prize',
#                'traveler_choice', 'image_list']

get_key = toolbox.Common.GetKey()
get_key.update_priority({
    'default': {
        'daodao': 10,
        'qyer': 9,
        'yelp': 8,
        'tripadvisor': 10
    }
})


def get_attr_dict(city_id):
    """
    get all attraction info in the city
    :param city_id: city mioji id
    :return: city info dict
    """
    attr_dict = defaultdict(dict)

    # get s sid
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='attr_merge')
    with conn.cursor() as cursor:
        sql = '''SELECT
  source,
  source_id
FROM attr_unid
WHERE city_id = %s;'''
        cursor.execute(sql, (city_id,))
        res = ["('{0}', '{1}')".format(s, sid) for s, sid in cursor.fetchall()]
    conn.close()

    # get whole line in per city
    conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', passwd='mioji1109', charset='utf8', db='base_data')
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = "select * from attr where (source, id) in (%s)" % ','.join(res)

        cursor.execute(sql)
        for line in cursor.fetchall():
            source = line['source']
            source_id = line['id']
            attr_dict[(source, source_id)] = line
    conn.close()
    return attr_dict


def get_task():
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='attr_merge')
    # 获取所有用于融合的城市 id
    cursor = conn.cursor()
    cursor.execute("select distinct city_id from attr_unid where city_id=50012;")
    total_city_id = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()

    for each_city_id in total_city_id:
        print("City ID:", each_city_id)
        cursor = conn.cursor(cursor=DictCursor)
        city_rest_dict = defaultdict(list)
        sql = '''SELECT
  id,
  city_id,
  group_concat(concat(source, '|', source_id) SEPARATOR '|_||_|') AS union_info
FROM attr_unid WHERE city_id=%s
GROUP BY id'''
        cursor.execute(sql, (each_city_id,))
        for line in cursor.fetchall():
            miaoji_id = line['id']
            city_id = line['city_id']
            union_info = line['union_info']
            city_rest_dict[city_id].append((miaoji_id, city_id, union_info))
        yield city_rest_dict
        cursor.close()


if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='attr_merge')

    for task_dict in get_task():
        count = 0
        data = []
        # 获取融合城市信息
        for key, values in list(task_dict.items()):
            # get per city rest info
            rest_info_dict = get_attr_dict(key)
            for miaoji_id, city_id, union_info in values:
                # 初始化融合前变量
                data_dict = defaultdict(dict)
                can_be_used = False

                # 初始化融合信息
                for each_name in (json_name_list + norm_name_list + other_name_list):
                    data_dict[each_name] = {}

                # 遍历所有需要融合的 source 以及 id，并生成 dict 类融合内容
                for s_sid in union_info.split('|_||_|'):
                    source, source_id = s_sid.split('|')

                    # 未获得融合 id 信息
                    if not source_id or not source:
                        continue

                    # 未获得融合数据
                    rest_info = rest_info_dict[(source, source_id)]
                    if rest_info == {}:
                        continue

                    can_be_used = True

                    for each_name in (json_name_list + norm_name_list):
                        if str(rest_info[each_name]).upper() not in ('NULL', ''):
                            data_dict[each_name][source] = rest_info[each_name]

                # 不能融合的内容包含两种
                if not can_be_used:
                    print('union_info', union_info)
                    continue

                new_data_dict = {}
                for norm_name in norm_name_list:
                    new_data_dict[norm_name] = get_key.get_key_by_priority_or_default(data_dict[norm_name], norm_name,
                                                                                      '')

                for json_name in json_name_list:
                    new_data_dict[json_name] = json.dumps(data_dict[json_name])

                new_data_dict['phone'] = new_data_dict['phone'].replace('电话号码：', '').strip()

                # 数据操作部分
                # 添加 source
                source = '|'.join(sorted(data_dict['source'].values()))

                # add alias
                alias = '|'.join(filter(lambda x: x != data_dict['name'] and x != data_dict['name_en'],
                                        set(list(data_dict['name'].values()) +
                                            list(data_dict['name_en'].values()))
                                        )
                                 )

                # todo modify opentime, norm_tagid, comment and so on

                # 数据入库部分
                # 替换旧的 data_dict
                data_dict = new_data_dict

                sql = 'insert into chat_attraction(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
                      '`map_info`,`address`,`star`,`plantocount`,`beentocount`,`real_ranking`,' \
                      '`grade`,`commentcount`,`tagid`,`url`,`website_url`,`phone`,`introduction`,' \
                      '`open_desc`,`recommend_lv`,`prize`,`traveler_choice`, `alias`, ' \
                      '`image`, `ori_grade`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
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
                    data_dict['traveler_choice'], alias, data_dict['imgurl'], data_dict['grade'])
                )

                if count % 3000 == 0:
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
    conn.close()