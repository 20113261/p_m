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
import dataset
import time
from toolbox.Common import is_legal, is_chinese
from pymysql.cursors import DictCursor
from collections import defaultdict
from Config.settings import attr_data_conf, attr_merge_conf, attr_final_conf, rest_data_conf, rest_merge_conf, \
    shop_merge_conf, shop_data_conf, poi_data_conf
from add_open_time.fix_daodao_time import fix_daodao_open_time
from norm_tag.attr_norm_tag import tradition2simple
# from norm_tag.rest_norm_tag import get_norm_tag as rest_get_norm_tag
# from norm_tag.shop_norm_tag import get_norm_tag as shop_get_norm_tag
from norm_tag.norm_tag import get_norm_tag
from get_near_city.get_near_city import get_nearby_city
from logger import func_time_logger, get_logger
from service_platform_conn_pool import poi_ori_pool, data_process_pool, base_data_pool
from toolbox.Common import is_legal
from poi_ori.already_merged_city import update_already_merge_city
from poi_ori.unknown_keywords import insert_unknown_keywords

logger = get_logger("insert_poi_log")

need_cid_file = True

W2N = {
    '¥¥-¥¥¥': '23',
    '¥': '1',
    '': '0',
    '¥¥¥¥': '4'
}

get_key = toolbox.Common.GetKey()
get_key.update_priority({
    'default': {
        'mioji': 15,
        'daodao': 10,
        'tripadvisor': 10,
        'qyer': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
    ('name', 'name_en'): {
        'mioji': 15,
        'qyer': 10,
        'daodao': 9,
        'tripadvisor': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
    ('address', 'opentime', 'grade', 'star'): {
        'mioji': 15,
        'daodao': 10,
        'tripadvisor': 10,
        'qyer': 9,
        'yelp': 8,
        'mafengwo': 7,
    },
})

poi_type = None
merge_conf = None
data_conf = None
others_name_list = None
json_name_list = None
norm_name_list = None
data_process_table_name = None


def init_global_name(_poi_type):
    global poi_type
    global merge_conf
    global data_conf
    global others_name_list
    global json_name_list
    global norm_name_list
    global data_process_table_name

    poi_type = _poi_type
    if _poi_type == 'attr':
        data_process_table_name = 'chat_attraction'
        merge_conf = attr_merge_conf
        data_conf = poi_data_conf
        others_name_list = ['source']
        json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts',
                          'tagid',
                          'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone', 'opentime',
                          'prize',
                          'traveler_choice', 'imgurl']
    elif _poi_type == 'rest':
        data_process_table_name = 'chat_restaurant'
        merge_conf = rest_merge_conf
        data_conf = poi_data_conf
        others_name_list = ['source']
        json_name_list = ['commentcounts', 'ranking', 'price', 'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'url', 'phone', 'opentime',
                          'prize', 'traveler_choice', 'price_level', 'cuisines', 'imgurl']
    elif _poi_type == 'shop':
        data_process_table_name = 'chat_shopping'
        merge_conf = shop_merge_conf
        data_conf = poi_data_conf
        others_name_list = ['source']

        json_name_list = ['url', 'ranking', 'star', 'recommend_lv', 'plantocounts', 'beentocounts', 'commentcounts',
                          'tagid',
                          'introduction']
        norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'site', 'phone',
                          'opentime', 'prize', 'traveler_choice', 'imgurl']
    else:
        raise TypeError("Unknown Type: {}".format(_poi_type))


@func_time_logger
def get_poi_dict(city_id):
    """
    get all attraction info in the city
    :param city_id: city mioji id
    :return: city info dict
    """
    _poi_dict = defaultdict(dict)
    # get s sid
    #     conn = poi_ori_pool.connection()
    #     cursor = conn.cursor()
    #     sql = '''SELECT
    # source,
    # source_id
    # FROM {}_unid
    # WHERE city_id = '{}';'''.format(poi_type, city_id)
    #     cursor.execute(sql)
    #     source_res = []
    #     online_res = []
    #     for s, sid in cursor.fetchall():
    #         if s == 'online':
    #             online_res.append(sid)
    #         else:
    #             source_res.append("('{0}', '{1}')".format(s, sid))
    #     cursor.close()
    #     conn.close()

    _online_data = {}
    # get whole data process
    conn = data_process_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    sql = '''SELECT *
FROM {}
WHERE city_id='{}';'''.format(data_process_table_name, city_id)
    _t = time.time()
    cursor.execute(sql)
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    for each in cursor.fetchall():
        _online_data[each['id']] = each
    cursor.close()
    conn.close()

    # get whole base data, and update data process result
    conn = base_data_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    sql = '''SELECT *
    FROM {}
    WHERE city_id='{}';'''.format(data_process_table_name, city_id)
    _t = time.time()
    cursor.execute(sql)
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    for each in cursor.fetchall():
        # gevent 中 pop 有问题，跳过 pop 的使用
        # each.pop('tag_id')
        _e = {}
        for k, v in each.items():
            if k != 'tag_id':
                _e[k] = v
        _online_data[each['id']].update(_e)
    cursor.close()
    conn.close()

    # get whole source data
    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    sql = "select * from {} where city_id='{}'".format(poi_type, city_id)
    _t = time.time()
    cursor.execute(sql)
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    for line in cursor.fetchall():
        source = line['source']
        source_id = line['id']
        _poi_dict[(source, source_id)] = line
    cursor.close()
    conn.close()
    return _poi_dict, _online_data


@func_time_logger
def get_poi_union_info(cid):
    conn = poi_ori_pool.connection()
    # 获取所有用于融合的城市 id

    logger.info("[get city task][cid: {}]".format(cid))
    cursor = conn.cursor(cursor=DictCursor)
    city_poi = []
    sql = '''SELECT
id,
city_id,
group_concat(concat(source, '|', source_id) SEPARATOR '|_||_|') AS union_info
FROM {}_unid WHERE city_id='{}'
GROUP BY id'''.format(poi_type, cid)
    _t = time.time()
    cursor.execute(sql)
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    for line in cursor.fetchall():
        miaoji_id = line['id']
        city_id = line['city_id']
        union_info = line['union_info']
        city_poi.append((miaoji_id, city_id, union_info))
    cursor.close()
    conn.close()
    return city_poi


def add_open_time_filter(_v):
    if not is_legal(_v):
        return False
    try:
        _open_time = fix_daodao_open_time(_v)
        if is_legal(_open_time):
            return True
    except Exception:
        # todo 保存不能识别的 open time
        # insert_unknown_keywords('opentime', _v)
        logger.debug("[unknown open time][data: {}]".format(_v))
    return False


@func_time_logger
def poi_insert_data(cid, _poi_type):
    init_global_name(_poi_type)
    '''
    数据最终入库表
    if _poi_type == 'attr':
        sql = 'replace into chat_attraction(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
              '`map_info`,`address`,`star`,`plantocount`,`beentocount`,`real_ranking`,' \
              '`grade`,`commentcount`,`tagid`,`norm_tagid`,`norm_tagid_en`,`url`,`website_url`,`phone`,`introduction`,' \
              '`open`, `open_desc`,`recommend_lv`,`prize`,`traveler_choice`, `alias`, ' \
              '`image`, `ori_grade`,`nearCity`, `ranking`,`rcmd_open`,`add_info`,`address_en`,`event_mark`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,-1,"","","","")'
    elif _poi_type == 'rest':
        sql = 'replace into chat_restaurant(`id`,`name`,`name_en`,' \
              '`source`,`city_id`,`map_info`,`address`,`real_ranking`,' \
              '`grade`,`res_url`,`telphone`,`introduction`,`open_time`,`open_time_desc`,`prize`,' \
              '`traveler_choice`,`review_num`,`price`,`price_level`,`cuisines`, ' \
              '`image_urls`,`tagid`,`norm_tagid`,`norm_tagid_en`,`nearCity`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    elif _poi_type == 'shop':
        sql = 'replace into ' \
              'chat_shopping(`id`,`name`,`name_en`,`data_source`,`city_id`,' \
              '`map_info`,`address`,`star`,`plantocount`,`beentocount`,' \
              '`real_ranking`,`grade`,`commentcount`,`tagid`,`norm_tagid`,`norm_tagid_en`,`url`,`website_url`,' \
              '`phone`,`introduction`,`open`,`open_desc`,`recommend_lv`,`prize`,' \
              '`traveler_choice`,`image`,`nearCity`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
              '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))
    '''

    db = dataset.connect("mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/poi_merge?charset=utf8")
    table = db[data_process_table_name]
    conn = poi_ori_pool.connection()
    # for task_dict in get_task(cid):
    count = 0
    data = []

    # 获取融合需要的 poi 信息
    _info_dict, _online_data = get_poi_dict(cid)
    _city_poi = get_poi_union_info(cid)
    # 开始数据融合
    for miaoji_id, city_id, union_info in _city_poi:
        # 初始化融合前变量
        data_dict = defaultdict(dict)
        can_be_used = False

        # 获取线上环境数据
        o_data = _online_data.get(miaoji_id, None)

        # 初始化融合信息
        for each_name in (json_name_list + norm_name_list + others_name_list):
            data_dict[each_name] = {}
            if o_data is not None:
                if each_name in json_name_list:
                    if each_name in o_data:
                        try:
                            _res = json.loads(o_data[each_name])
                            if isinstance(_res, dict):
                                data_dict[each_name] = _res
                            else:
                                pass
                        except Exception:
                            pass
                else:
                    data_dict[each_name]['mioji'] = o_data.get(each_name, {})

        # 遍历所有需要融合的 source 以及 id，并生成 dict 类融合内容
        for s_sid in union_info.split('|_||_|'):
            source, source_id = s_sid.split('|')

            # todo 增加 online 的处理，先 load data，然后进行数据更新
            # todo 使用 online 的 base data 更新 data process 的字段

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
            logger.debug('[union_info: {}]'.format(union_info))
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
        unknown_tag = set()
        if 'daodao' in data_dict['tagid']:
            try:
                daodao_tagid, daodao_tagid_en, _unknown_tag = get_norm_tag(data_dict['tagid']['daodao'],
                                                                           poi_type)
                unknown_tag.update(_unknown_tag)
            except Exception:
                daodao_tagid, daodao_tagid_en = '', ''
        else:
            daodao_tagid, daodao_tagid_en = '', ''

        if 'qyer' in data_dict['tagid']:
            try:
                qyer_tagid, qyer_tagid_en, _unknown_tag = get_norm_tag(data_dict['tagid']['qyer'], poi_type)
                unknown_tag.update(_unknown_tag)
            except Exception:
                qyer_tagid, qyer_tagid_en = '', ''
        else:
            qyer_tagid, qyer_tagid_en = '', ''

        l_norm_tag = []
        l_norm_tag_en = []
        l_norm_tag.extend(daodao_tagid.split('|'))
        l_norm_tag_en.extend(daodao_tagid_en.split('|'))
        l_norm_tag.extend(qyer_tagid.split('|'))
        l_norm_tag_en.extend(qyer_tagid_en.split('|'))

        # 去除空 tag 以及重复 tag
        norm_tag = '|'.join(filter(lambda x: is_legal(x), set(l_norm_tag)))
        norm_tag_en = '|'.join(filter(lambda x: is_legal(x), set(l_norm_tag_en)))

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

        # 餐厅的 price_level 单独处理
        if poi_type == 'rest':
            data_dict['price_level'] = W2N.get(data_dict.get('price_level', ''), '0')

        # 添加 nearCity 字段
        nearby_city = get_nearby_city(poi_city_id=city_id, poi_map_info=data_dict['map_info'])
        ''''`image`, `ori_grade`,`nearCity`, `ranking`,`rcmd_open`,`add_info`,`address_en`,`event_mark`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
  '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,-1,"","","","")';'''

        # 数据清理以及入库部分
        # 全量经纬度不符合规范数据清理
        try:
            lng, lat = data_dict['map_info'].split(',')
            lng = float(lng)
            lat = float(lat)
            data_dict['map_info'] = '{},{}'.format(lng, lat)
        except Exception as exc:
            logger.exception(msg="[map_info filter error][data: {}]".format(data_dict['map_info']),
                             exc_info=exc)
            continue

        # 清理名称中的多余字符
        data_dict['name'] = data_dict['name'].replace('这是您的企业吗？', '').strip()
        name = data_dict['name']
        name_en = data_dict['name_en']
        if name_en in name and name_en != name:
            name.replace(name_en, '')

        # 字段修改部分
        # name
        if data_dict['name'].lower() in ('', 'null', '0'):
            data_dict['name'] = data_dict['name_en']

        # address
        if data_dict['address'].lower() in ('null', '0'):
            data_dict['address'] = ''

        # open time
        if norm_open_time.lower() in ('', 'null', '0'):
            if poi_type in ('attr', 'rest'):
                norm_open_time = '<*><*><00:00-23:55><SURE>'
            else:
                norm_open_time = '<*><*><08:00-20:00><SURE>'

        # todo 保存不能识别的 tag 以及 open time 信息
        if unknown_tag:
            # insert_unknown_keywords('tag', unknown_tag)
            logger.debug("[unknown tag][tags: {}]".format(unknown_tag))

        if poi_type == 'attr':
            per_data = {
                'id': miaoji_id,
                'name': data_dict['name'],
                'name_en': data_dict['name_en'],
                'data_source': source,
                'city_id': city_id,
                'map_info': data_dict['map_info'],
                'address': data_dict['address'],
                'star': data_dict['star'],
                'plantocount': data_dict['plantocounts'],
                'beentocount': data_dict['beentocounts'],
                'real_ranking': data_dict['ranking'],
                'grade': data_dict['grade'],
                'commentcount': data_dict['commentcounts'],
                'tagid': data_dict['tagid'],
                'norm_tagid': norm_tag,
                'norm_tagid_en': norm_tag_en,
                'url': data_dict['url'],
                'website_url': data_dict['site'],
                'phone': data_dict['phone'],
                'introduction': data_dict['introduction'],
                'open': norm_open_time,
                'open_desc': data_dict['opentime'],
                'recommend_lv': data_dict['recommend_lv'],
                'prize': data_dict['prize'],
                'traveler_choice': data_dict['traveler_choice'],
                'alias': alias,
                'image': data_dict['imgurl'],
                'ori_grade': data_dict['ori_grade'],
                'nearCity': nearby_city,
                'status_online': 'Open',
                'status_test': 'Open'
            }
            if not o_data:
                per_data.update({
                    'ranking': -1.0,
                    'rcmd_open': '',
                    'add_info': '',
                    'address_en': '',
                    'event_mark': ''
                })

            # 景点游览部分清理
            try:
                tagid_data = json.loads(data_dict['tagid'])
                if 'daodao' in tagid_data:
                    if is_legal(tagid_data['daodao']):
                        if '游览' in tagid_data['daodao']:
                            logger.debug("[tour filter][data: {}]".format(tagid_data['daodao']))
                            continue
            except Exception as exc:
                logger.exception(msg="[tour filter error]", exc_info=exc)

            data.append(per_data)
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
                data_dict['imgurl'], data_dict['tagid'], norm_tag, norm_tag_en, nearby_city))
        elif poi_type == 'shop':
            per_data = {
                'id': miaoji_id,
                'name': data_dict['name'],
                'name_en': data_dict['name_en'],
                'data_source': source,
                'city_id': city_id,
                'map_info': data_dict['map_info'],
                'address': data_dict['address'],
                'star': data_dict['star'],
                'plantocount': data_dict['plantocounts'],
                'beentocount': data_dict['beentocounts'],
                'real_ranking': data_dict['ranking'],
                'grade': data_dict['grade'],
                'commentcount': data_dict['commentcounts'],
                'tagid': data_dict['tagid'],
                'norm_tagid': norm_tag,
                'norm_tagid_en': norm_tag_en,
                'url': data_dict['url'],
                'website_url': data_dict['site'],
                'phone': data_dict['phone'],
                'introduction': data_dict['introduction'],
                'open': norm_open_time,
                'open_desc': data_dict['opentime'],
                'recommend_lv': data_dict['recommend_lv'],
                'prize': data_dict['prize'],
                'traveler_choice': data_dict['traveler_choice'],
                'image': data_dict['imgurl'],
                'nearCity': nearby_city,
                'status_online': 'Open',
                'status_test': 'Open'
            }
            if not o_data:
                per_data.update({
                    'ranking': -1.0,
                    'rcmd_open': '',
                    'image_list': '',
                })
            shopping_tag = ['礼品与特产商店', '大型购物中心', '农贸市场', '跳蚤市场与街边市场', '古董店', '百货商场', '厂家直营店', '购物']
            important_shopping_tag = ['礼品与特产商店', '大型购物中心', '百货商场', '厂家直营店', '购物']

            # 购物数据过滤，通过 tag 过滤
            tag_list = norm_tag.split('|')
            if not all([tag.strip() in shopping_tag for tag in tag_list]):
                if not any([tag.strip() in important_shopping_tag for tag in tag_list]):
                    continue

            table.upsert(per_data, keys=['id'])
            data.append(per_data)
        else:
            raise TypeError("Unknown Type: {}".format(poi_type))

        if count % 3000 == 0:
            _insert = 0
            logger.debug("Total: {}".format(count))
            _t = time.time()
            for d in data:
                _res = table.upsert(d, keys=['id'])
                if _res:
                    _insert += 1
            logger.debug(
                '[data upsert][count: {}][insert: {}][takes: {}]'.format(count, _insert, time.time() - _t))
            logger.debug("Insert: {}".format(_insert))
            db.commit()
            data = []
        count += 1

    logger.debug("Total: {}".format(count))
    _insert = 0
    for d in data:
        _res = table.upsert(d, keys=['id'])
        if _res:
            _insert += 1
    logger.debug("Insert: {}".format(_insert))
    db.commit()
    logger.debug("Insert: {}".format(_insert))
    conn.close()
    update_already_merge_city("{}_data".format(poi_type), cid)


if __name__ == '__main__':
    poi_insert_data(10001, 'attr')
