#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 上午10:25
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_insert_db.py
# @Software: PyCharm
import toolbox.Common
import pymysql
import json
import copy
from toolbox.Common import is_legal, is_chinese
from pymysql.cursors import DictCursor
from collections import defaultdict
from Config.settings import attr_data_conf, attr_merge_conf, attr_final_conf, rest_data_conf, rest_merge_conf, \
    shop_merge_conf, shop_data_conf
from add_open_time.fix_daodao_time import fix_daodao_open_time
from norm_tag.attr_norm_tag import tradition2simple, get_norm_tag as attr_get_norm_tag
from norm_tag.rest_norm_tag import get_norm_tag as rest_get_norm_tag
from norm_tag.shop_norm_tag import get_norm_tag as shop_get_norm_tag

need_cid_file = True

get_key = toolbox.Common.GetKey()
get_key.update_priority({
    'default': {
        'daodao': 10,
        'tripadvisor': 10,
        'qyer': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
    ('name', 'name_en'): {
        'qyer': 10,
        'daodao': 9,
        'tripadvisor': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
    ('address', 'opentime', 'grade', 'star'): {
        'daodao': 10,
        'tripadvisor': 10,
        'qyer': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
})

poi_type = 'rest'

if poi_type == 'attr':
    merge_conf = attr_merge_conf
    data_conf = attr_data_conf
    get_norm_tag = attr_get_norm_tag
elif poi_type == 'rest':
    merge_conf = rest_merge_conf
    data_conf = rest_data_conf
    get_norm_tag = rest_get_norm_tag
elif poi_type == 'shop':
    merge_conf = shop_merge_conf
    data_conf = shop_data_conf
    get_norm_tag = shop_get_norm_tag
else:
    raise TypeError("Unknown Type: {}".format(poi_type))


def get_poi_dict(city_id):
    """
    get all attraction info in the city
    :param city_id: city mioji id
    :return: city info dict
    """

    # get s sid
    _poi_dict = defaultdict(dict)
    conn = pymysql.connect(**merge_conf)
    with conn.cursor() as cursor:
        sql = '''SELECT
  source,
  source_id
FROM {}_unid
WHERE city_id = %s;'''.format(poi_type)
        cursor.execute(sql, (city_id,))
        res = ["('{0}', '{1}')".format(s, sid) for s, sid in cursor.fetchall()]
    conn.close()

    # get line old
    if poi_type != 'rest':
        conn = pymysql.connect(**merge_conf)
        with conn.cursor(cursor=DictCursor) as cursor:
            sql = "select * from {} where (source, id) in ({})".format(poi_type, ','.join(res))

            cursor.execute(sql)
            for line in cursor.fetchall():
                source = line['source']
                source_id = line['id']
                _poi_dict[(source, source_id)] = line

    # get whole line in per city
    conn = pymysql.connect(**data_conf)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = "select * from {} where (source, id) in ({})".format(poi_type, ','.join(res))
        cursor.execute(sql)
        for line in cursor.fetchall():
            source = line['source']
            source_id = line['id']
            _poi_dict[(source, source_id)] = line
    conn.close()
    return _poi_dict


def get_task():
    conn = pymysql.connect(**merge_conf)
    # 获取所有用于融合的城市 id
    cursor = conn.cursor()
    cursor.execute("select distinct city_id from {}_unid where city_id in ({});".format(poi_type, format(
        ','.join((map(lambda x: x.strip(), open('cid_file')))))))
    total_city_id = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()

    for each_city_id in total_city_id:
        print("City ID:", each_city_id)
        cursor = conn.cursor(cursor=DictCursor)
        city_poi_dict = defaultdict(list)
        sql = '''SELECT
  id,
  city_id,
  group_concat(concat(source, '|', source_id) SEPARATOR '|_||_|') AS union_info
FROM {}_unid WHERE city_id=%s
GROUP BY id'''.format(poi_type)
        cursor.execute(sql, (each_city_id,))
        for line in cursor.fetchall():
            miaoji_id = line['id']
            city_id = line['city_id']
            union_info = line['union_info']
            city_poi_dict[city_id].append((miaoji_id, city_id, union_info))
        yield city_poi_dict
        cursor.close()


def add_open_time_filter(_v):
    if not is_legal(_v):
        return False
    try:
        _open_time = fix_daodao_open_time(_v)
        if is_legal(_open_time):
            return True
    except Exception:
        print(_v)
    return False


def insert_data(_poi_type):
    if _poi_type == 'attr':
        others_name_list = ['source']
        json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts',
                          'tagid',
                          'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone', 'opentime',
                          'prize',
                          'traveler_choice', 'imgurl']
    elif _poi_type == 'rest':
        others_name_list = ['source']
        json_name_list = ['commentcounts', 'ranking', 'price', 'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'url', 'phone', 'opentime',
                          'prize', 'traveler_choice', 'price_level', 'cuisines', 'imgurl']
    elif _poi_type == 'shop':
        others_name_list = ['source']

        json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts',
                          'tagid',
                          'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone',
                          'opentime', 'prize', 'traveler_choice', 'imgurl']
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))

    # 数据最终入库表
    if _poi_type == 'attr':
        sql = 'replace into chat_attraction(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
              '`map_info`,`address`,`star`,`plantocount`,`beentocount`,`real_ranking`,' \
              '`grade`,`commentcount`,`tagid`,`norm_tagid`,`norm_tagid_en`,`url`,`website_url`,`phone`,`introduction`,' \
              '`open`, `open_desc`,`recommend_lv`,`prize`,`traveler_choice`, `alias`, ' \
              '`image`, `ori_grade`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    elif _poi_type == 'rest':
        sql = 'replace into chat_restaurant(`id`,`name`,`name_en`,' \
              '`source`,`city_id`,`map_info`,`address`,`real_ranking`,' \
              '`grade`,`res_url`,`telphone`,`introduction`,`open_time`,`open_time_desc`,`prize`,' \
              '`traveler_choice`,`review_num`,`price`,`price_level`,`cuisines`, ' \
              '`image_urls`,`tagid`,`norm_tagid`,`norm_tagid_en`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    elif _poi_type == 'shop':
        sql = 'replace into ' \
              'chat_shopping(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
              '`map_info`,`address`,`star`,`plantocount`,`beentocount`,' \
              '`real_ranking`,`grade`,`commentcount`,`tagid`,`norm_tagid`,`norm_tagid_en`,`url`,`website_url`,' \
              '`phone`,`introduction`,`open`,`open_desc`,`recommend_lv`,`prize`,' \
              '`traveler_choice`,`image`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))

    conn = pymysql.connect(**merge_conf)
    for task_dict in get_task():
        count = 0
        data = []
        # 获取融合城市信息
        for key, values in list(task_dict.items()):
            # get per city rest info
            _info_dict = get_poi_dict(key)
            for miaoji_id, city_id, union_info in values:
                # 初始化融合前变量
                data_dict = defaultdict(dict)
                can_be_used = False

                # 初始化融合信息
                for each_name in (json_name_list + norm_name_list + others_name_list):
                    data_dict[each_name] = {}

                # 遍历所有需要融合的 source 以及 id，并生成 dict 类融合内容
                for s_sid in union_info.split('|_||_|'):
                    source, source_id = s_sid.split('|')

                    # 未获得融合 id 信息
                    if not source_id or not source:
                        continue

                    # 未获得融合数据
                    poi_info = _info_dict[(source, source_id)]
                    if poi_info == {}:
                        continue

                    can_be_used = True

                    # 加 key
                    for each_name in (json_name_list + norm_name_list):
                        if is_legal(poi_info[each_name]):
                            if isinstance(poi_info[each_name], str):
                                data_dict[each_name][source] = tradition2simple(poi_info[each_name]).decode()
                            else:
                                data_dict[each_name][source] = poi_info[each_name]

                    # 补空白的内容
                    for each_name in (json_name_list + norm_name_list):
                        if each_name not in data_dict:
                            data_dict[each_name] = {}

                # 不能融合的内容包含两种
                if not can_be_used:
                    print('union_info', union_info)
                    continue

                new_data_dict = {}
                for norm_name in norm_name_list:
                    new_data_dict[norm_name] = get_key.get_key_by_priority_or_default(data_dict[norm_name], norm_name,
                                                                                      '')

                # daodao url 处理
                if 'daodao' in data_dict['url']:
                    data_dict['url']['daodao'] = data_dict['url']['daodao'].replace('www.tripadvisor.com.hk',
                                                                                    'www.tripadvisor.cn')

                # 餐厅使用 cuisines 添加 tagid
                if poi_type == 'rest':
                    data_dict['tagid'] = copy.deepcopy(data_dict['cuisines'])
                    new_data_dict['tagid'] = json.dumps(data_dict['tagid'])

                for json_name in json_name_list:
                    new_data_dict[json_name] = json.dumps(data_dict[json_name])

                new_data_dict['phone'] = new_data_dict['phone'].replace('电话号码：', '').strip()

                # 数据操作部分
                # 添加 source
                source = '|'.join(map(lambda x: x.split('|')[0], union_info.split('|_||_|')))

                # add alias
                alias = '|'.join(filter(lambda x: x != new_data_dict['name'] and x != new_data_dict['name_en'],
                                        set(list(data_dict['name'].values()) +
                                            list(data_dict['name_en'].values()))
                                        )
                                 )

                # ori_grade modify
                new_data_dict['ori_grade'] = json.dumps(data_dict['grade'])

                # add open time
                final_open_time_desc = get_key.get_key_by_priority_or_default(data_dict['opentime'], 'opentime',
                                                                              special_filter=add_open_time_filter)
                if final_open_time_desc:
                    norm_open_time = fix_daodao_open_time(final_open_time_desc)
                else:
                    norm_open_time = ''

                # add norm tag
                # todo change make qyer and other can be used
                if 'daodao' in data_dict['tagid']:
                    daodao_tagid, daodao_tagid_en = get_norm_tag(data_dict['tagid'])
                else:
                    daodao_tagid, daodao_tagid_en = '', ''

                if 'qyer' in data_dict['tagid']:
                    qyer_tagid, qyer_tagid_en = get_norm_tag(data_dict['tagid'])
                else:
                    qyer_tagid, qyer_tagid_en = '', ''

                l_norm_tag = []
                l_norm_tag_en = []
                l_norm_tag.extend(daodao_tagid.split('|'))
                l_norm_tag_en.extend(daodao_tagid_en.split('|'))
                l_norm_tag.extend(qyer_tagid.split('|'))
                l_norm_tag_en.extend(qyer_tagid_en.split('|'))

                norm_tag = '|'.join(l_norm_tag)
                norm_tag_en = '|'.join(l_norm_tag_en)

                # 数据入库部分
                # 替换旧的 data_dict
                data_dict = new_data_dict

                # name name_en 判断
                if data_dict['name'] != data_dict['name_en']:
                    if data_dict['name_en'] in data_dict['name']:
                        data_dict['name'] = data_dict['name'].replace(data_dict['name_en'], '')

                # 通过 name_en 回添 name
                if data_dict['name'] == '':
                    data_dict['name'] = data_dict['name_en']

                # phone 处理
                if data_dict['phone'] in ('+ 新增電話號碼', '+ 新增电话号码'):
                    data_dict['phone'] = ''

                if poi_type == 'attr':
                    data.append((
                        miaoji_id, data_dict['name'], data_dict['name_en'], source, city_id,
                        data_dict['map_info'], data_dict['address'],
                        data_dict['star'], data_dict['plantocounts'], data_dict['beentocounts'],
                        data_dict['ranking'], data_dict['grade'],
                        data_dict['commentcounts'],
                        data_dict['tagid'], norm_tag, norm_tag_en, data_dict['url'], data_dict['site'],
                        data_dict['phone'],
                        data_dict['introduction'], norm_open_time,
                        data_dict['opentime'], data_dict['recommend_lv'], data_dict['prize'],
                        data_dict['traveler_choice'], alias, data_dict['imgurl'], data_dict['ori_grade'])
                    )
                elif poi_type == 'rest':
                    data.append((
                        miaoji_id, data_dict['name'], data_dict['name_en'], source, key,
                        data_dict['map_info'],
                        data_dict['address'],
                        data_dict['ranking'], data_dict['grade'],
                        data_dict['url'], data_dict['phone'], data_dict['introduction'], norm_open_time,
                        data_dict['opentime'],
                        data_dict['prize'], data_dict['traveler_choice'],
                        data_dict['commentcounts'], data_dict['price'], data_dict['price_level'],
                        data_dict['cuisines'],
                        data_dict['imgurl'], data_dict['tagid'], norm_tag, norm_tag_en))
                elif poi_type == 'shop':
                    data.append((
                        miaoji_id, data_dict['name'], data_dict['name_en'], source, city_id,
                        data_dict['map_info'], data_dict['address'],
                        data_dict['star'], data_dict['plantocounts'], data_dict['beentocounts'],
                        data_dict['ranking'], data_dict['grade'],
                        data_dict['commentcounts'],
                        data_dict['tagid'], norm_tag, norm_tag_en, data_dict['url'], data_dict['site'],
                        data_dict['phone'],
                        data_dict['introduction'], norm_open_time,
                        data_dict['opentime'], data_dict['recommend_lv'], data_dict['prize'],
                        data_dict['traveler_choice'],
                        data_dict['imgurl']))
                else:
                    raise TypeError("Unknown Type: {}".format(poi_type))

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


if __name__ == '__main__':
    insert_data(poi_type)
