#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/13 下午7:07
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_ori.py
# @Software: PyCharm
import re
import time
import json
import redis
import threading
from collections import defaultdict
from toolbox.Common import is_legal
from data_source import MysqlSource
from service_platform_conn_pool import base_data_pool, poi_ori_pool
from logger import get_logger, func_time_logger
from poi_ori.already_merged_city import update_already_merge_city

poi_type = None
online_table_name = None
data_source_table = None
white_list = list()

max_id = None
lock = threading.Lock()
logger = get_logger("poi_ori")


def init_global_name(_poi_type):
    global poi_type
    global online_table_name
    global data_source_table
    poi_type = _poi_type
    if poi_type == 'attr':
        online_table_name = 'chat_attraction'
        data_source_table = 'attr'
    elif poi_type == 'rest':
        online_table_name = 'chat_restaurant'
        data_source_table = 'rest'
    elif poi_type == 'shop':
        online_table_name = 'chat_shopping'
        data_source_table = 'shop'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))


onlinedb = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

data_db = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'poi_merge'
}

'''
| - city 每个函数只融合一个城市，函数中内容从 poi 开始
| -  -  - poi
| -  -  -  -  merge_key set(记录全量的融合索引)
| -  -  -  -  merged_source_and_sid  set(记录原始信息的 source、sid) 
'''

_id = 0

r = redis.Redis(host='10.10.180.145')


def get_max_id():
    global poi_type
    global online_table_name
    global data_source_table
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT max(id) FROM {};'''.format(online_table_name))
    _id_online = cursor.fetchone()[0]
    conn.close()

    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT max(id) FROM {}_unid;'''.format(poi_type))
    _id_merged = cursor.fetchone()[0]
    conn.close()
    if _id_merged and _id_online:
        return max(_id_online, _id_merged)
    elif _id_merged:
        return _id_merged
    elif _id_online:
        return _id_online
    else:
        raise Exception("无法获取最大 id ，不能进行融合")


def get_max_uid():
    # todo add func get max uid
    # todo each type attr rest shop
    # todo insert id into redis
    # mk final
    global poi_type
    with lock:
        global max_id
        if not max_id:
            max_id = get_max_id()
        if poi_type == 'attr':
            max_id = 'v' + str(int(max_id[1:]) + 1)
        elif poi_type == 'shop':
            max_id = 'sh' + str(int(max_id[2:]) + 1)
        elif poi_type == 'rest':
            max_id = 'r' + str(int(max_id[1:]) + 1)
        else:
            raise TypeError("Unknown Type: {}".format(poi_type))
        return max_id


@func_time_logger
def init_white_list():
    global white_list
    if white_list:
        return
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT info
FROM white_list
WHERE type = %s;''', (poi_type,))

    for line in cursor.fetchall():
        _d = set()
        if not line:
            continue
        else:
            _data = json.loads(line[0])
            if 'qyer' in _data:
                _d.add(('qyer', str(_data['qyer'])))
                _d.add(('qyer_url_id', str(_data['qyer_url_id'])))
            if 'daodao' in _data:
                _d.add(('daodao', str(_data['daodao'])))
        white_list.append(_d)
    cursor.close()
    conn.close()


@func_time_logger
def get_data(cid_or_geohash):
    global poi_type
    global online_table_name
    global data_source_table
    """
    返回各优先级数据，用于融合时使用，按照 线上数据 > 各源数据 ( 暂时无源内部的排序 ) 的提取规则由先向后进行数据提取
    source = ''
    sid = ''
    keys = set()
    yield source, sid, keys
    """

    # 线上数据，按照优先级排序，先进入的数据优先使用 id
    _t = time.time()
    if poi_type == 'attr':
        sql = '''SELECT id, name, name_en, alias, url
    FROM {1}
    WHERE city_id = '{0}'
    ORDER BY status_online DESC, status_test DESC, official DESC, grade;'''.format(cid_or_geohash, online_table_name)
    elif poi_type == 'rest':
        sql = '''SELECT id, name, name_en, url
        FROM {1}
        WHERE city_id = '{0}'
        ORDER BY status_online DESC, status_test DESC, official DESC, grade;'''.format(cid_or_geohash,
                                                                                       online_table_name)
    elif poi_type == 'shop':
        sql = '''SELECT id, name, name_en, url
        FROM {1}
        WHERE city_id = '{0}'
        ORDER BY status_online DESC, status_test DESC, official DESC, grade;'''.format(cid_or_geohash,
                                                                                       online_table_name)
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))
    for data in MysqlSource(onlinedb, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
        keys = set()
        if is_legal(data['name']):
            keys.add(data['name'])
        if is_legal(data['name_en']):
            keys.add(data['name_en'])
        _url = data['url']
        if _url:
            _j_url = json.loads(_url)
            if 'qyer' in _j_url:
                try:
                    _source = 'qyer'
                    _sid = re.findall('place.qyer.com/poi/([\s\S]+)/', _j_url['qyer'])[0]
                    keys.add((_source, _sid))
                except Exception:
                    pass
            if 'daodao' in _j_url:
                try:
                    _source = 'daodao'
                    _sid = re.findall('-d(\d+)', _j_url['daodao'])[0]
                    keys.add((_source, _sid))
                except Exception:
                    pass
        if poi_type == 'attr':
            for key in data['alias'].split('|'):
                if is_legal(key):
                    keys.add(key)
        logger.debug("[source: {}][sid: {}][keys: {}]".format('online', data['id'], keys))
        yield 'online', data['id'], keys
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))

    # 各源数据，暂时不增加排序规则
    _t = time.time()
    sql = '''SELECT
  id,
  source,
  name,
  name_en,
  url
FROM {1}
WHERE city_id = '{0}';'''.format(cid_or_geohash, data_source_table)
    for data in MysqlSource(data_db, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
        keys = set()
        if is_legal(data['name']):
            keys.add(data['name'])
        if is_legal(data['name_en']):
            keys.add(data['name_en'])
        if data['source'] == 'qyer':
            _sid = re.findall('place.qyer.com/poi/([\s\S]+)/', data['url'])[0]
            keys.add(('qyer', _sid))
        if data['source'] == 'daodao':
            _sid = re.findall('-d(\d+)', data['url'])[0]
            keys.add(('daodao', _sid))
        logger.debug("[source: {}][sid: {}][keys: {}]".format(data['source'], data['id'], keys))
        yield data['source'], data['id'], keys
    logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))


@func_time_logger
def insert_poi_unid(merged_dict, cid_or_geohash):
    global white_list
    global online_table_name
    global data_source_table
    start = time.time()
    # get city country name map_info
    _dev_conn = base_data_pool.connection()
    _dev_cursor = _dev_conn.cursor()
    _dev_cursor.execute('''SELECT
  city.id       AS cid,
  city.name     AS city_name,
  country.name  AS country_name,
  city.map_info AS map_info
FROM city
  JOIN country ON city.country_id = country.mid
WHERE city.id = {};'''.format(cid_or_geohash))
    cid, city_name, country, city_map_info = _dev_cursor.fetchone()
    _dev_cursor.close()
    _dev_conn.close()

    # 去除 total 的写法，费劲但是提升速度不明显，使用 total 后 5.9 秒获取巴黎全部信息，直接获取 6.9 秒，相差可以接受
    # init id list
    # online_ids = set()
    # data_ids = set()
    # for _, s_sid_set in merged_dict.items():
    #     for source, sid in s_sid_set:
    #         if source == 'online':
    #             online_ids.add(sid)
    #         else:
    #             data_ids.add((source, sid))

    # get data total
    # get online data name name_en map_info grade star ranking address url
    total_data = {}
    _dev_conn = base_data_pool.connection()
    _dev_cursor = _dev_conn.cursor()
    try:
        _t = time.time()
        sql = '''SELECT
  id,
  name,
  name_en,
  map_info,
  grade,
  -1,
  ranking,
  address,
  ''
FROM {} WHERE city_id='{}';'''.format(online_table_name, cid_or_geohash)
        _dev_cursor.execute(sql)
        logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    except Exception as exc:
        logger.exception("[sql exc][sql: {}]".format(sql), exc_info=exc)

    for line in _dev_cursor.fetchall():
        total_data[('online', line[0])] = line[1:]
    _dev_cursor.close()
    _dev_conn.close()

    # get poi name name_en map_info grade star ranking address url
    _data_conn = poi_ori_pool.connection()
    _data_cursor = _data_conn.cursor()
    try:
        _t = time.time()
        sql = '''SELECT
source,
id,
name,
name_en,
map_info,
grade,
star,
ranking,
address,
url
FROM {}
WHERE city_id='{}';'''.format(data_source_table, cid_or_geohash)
        _data_cursor.execute(sql)
        logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    except Exception as exc:
        logger.exception("[sql exc][sql: {}]".format(sql), exc_info=exc)
    for line in _data_cursor.fetchall():
        total_data[(line[0], line[1])] = line[2:]
    _data_cursor.close()
    _data_conn.close()

    # init white list total data
    if white_list:
        _s_sid = []
        for _each in white_list:
            _s_sid.extend(_each)

        _ori_conn = poi_ori_pool.connection()
        _ori_cursor = _ori_conn.cursor()
        try:
            _t = time.time()
            query_sql = '''SELECT
  source,
  id,
  name,
  name_en,
  map_info,
  grade,
  star,
  ranking,
  address,
  url
FROM {}
WHERE (source, id) IN ({});'''.format(data_source_table, ','.join(map(lambda x: "('{}', '{}')".format(*x), _s_sid)))
            _ori_cursor.execute(query_sql)
            logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
        except Exception as exc:
            logger.exception("[sql exc][sql: {}]".format(sql), exc_info=exc)
        for line in _ori_cursor.fetchall():
            total_data[(line[0], line[1])] = line[2:]
        _ori_cursor.close()
        _ori_conn.close()

    data = []
    for uid, s_sid_set in merged_dict.items():
        for source, sid in s_sid_set:
            # name name_en map_info grade star ranking address url
            name, name_en, map_info, grade, star, ranking, address, url = total_data[(source, sid)]
            if not is_legal(name):
                name = ''
            if not is_legal(name_en):
                name_en = ''
            if not is_legal(grade):
                grade = -1.0
            if not is_legal(star):
                star = -1.0
            if not is_legal(ranking):
                ranking = -1.0
            if not is_legal(address):
                address = ''
            if not is_legal(url):
                url = ''

            data.append((uid, cid, city_name, country, city_map_info, source, sid, name, name_en, map_info, grade,
                         star, ranking, address, url))

            #     data = []
            #     _dev_conn = base_data_pool.connection()
            #     _dev_cursor = _dev_conn.cursor()
            #     _data_conn = poi_ori_pool.connection()
            #     _data_cursor = _data_conn.cursor()
            #     for uid, s_sid_set in merged_dict.items():
            #         for source, sid in s_sid_set:
            #             if source == 'online':
            #                 _dev_cursor.execute('''SELECT
            #   name,
            #   name_en,
            #   map_info,
            #   grade,
            #   -1,
            #   ranking,
            #   address,
            #   ''
            # FROM chat_attraction
            # WHERE id = '{}';'''.format(sid))
            #                 try:
            #                     name, name_en, map_info, grade, star, ranking, address, url = _dev_cursor.fetchone()
            #                 except Exception as exc:
            #                     logger.exception("[error sql query][source: {}][sid: {}]".format(source, sid), exc_info=exc)
            #                     continue
            #             else:
            #                 _data_cursor.execute('''SELECT
            #   CASE WHEN name NOT IN ('NULL', '', NULL)
            #     THEN name
            #   ELSE '' END,
            #   CASE WHEN name_en NOT IN ('NULL', '', NULL)
            #     THEN name_en
            #   ELSE '' END,
            #   map_info,
            #   CASE WHEN grade NOT IN ('NULL', '', NULL)
            #     THEN grade
            #   ELSE -1.0 END AS grade,
            #   CASE WHEN star NOT IN ('NULL', '', NULL)
            #     THEN star
            #   ELSE -1.0 END AS star,
            #   CASE WHEN ranking NOT IN ('NULL', '', NULL)
            #     THEN ranking
            #   ELSE -1.0 END AS ranking,
            #   CASE WHEN address NOT IN ('NULL', '', NULL)
            #     THEN address
            #   ELSE '' END,
            #   CASE WHEN url NOT IN ('null', '', NULL)
            #     THEN url
            #   ELSE '' END
            # FROM attr
            # WHERE source = '{}' AND id = '{}';'''.format(source, sid))
            #                 try:
            #                     name, name_en, map_info, grade, star, ranking, address, url = _data_cursor.fetchone()
            #                 except Exception as exc:
            #                     logger.exception("[error sql query][source: {}][sid: {}]".format(source, sid), exc_info=exc)
            #                     continue
            #
            #             data.append((uid, cid, city_name, country, city_map_info, source, sid, name, name_en, map_info, grade,
            #                          star, ranking, address, url))
            #     _dev_cursor.close()
            #     _data_cursor.close()
            #     _dev_conn.close()
            #     _data_conn.close()

    _final_conn = poi_ori_pool.connection()
    _final_cursor = _final_conn.cursor()
    # for d in data:
    try:
        _t = time.time()
        sql = '''REPLACE INTO {}_unid (id, city_id, city_name, country_name, city_map_info, source, source_id, name, name_en, map_info, grade, star, ranking, address, url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''.format(poi_type)
        _final_cursor.executemany(
            sql,
            data
        )
        logger.debug('[query][sql: {}][takes: {}]'.format(sql, time.time() - _t))
    except Exception as exc:
        logger.exception("[insert unid table error]", exc_info=exc)
    _final_conn.commit()
    _final_cursor.close()
    _final_conn.close()

    logger.info("[finish prepare data][city: {}][line_count: {}][takes: {}]".format(cid_or_geohash, len(data),
                                                                                    time.time() - start))


@func_time_logger
def _poi_merge(cid_or_geohash):
    global online_table_name
    global data_source_table
    # 生成空的用于融合的相似字典存放数据
    similar_dict = defaultdict(set)
    merged_dict = defaultdict(set)

    '''在查 英国国家美术馆，奥斯陆国家美术馆 没有融合的 case 中发现当前融合逻辑的存在问题。
当前融合逻辑中会循环景点数据，当该景点能够融合时，进行融合，否则认为新增一条景点。
导致原因：
1、当前景点 A 与景点 B 不能进行融合，从而认定为新增的一条
2、当前景点 C 可以与 A 与 B 进行融合，但 A 与 B 已经为两条景点，C 只会与其中一条融合
最终需要结果：
A、B、C 进行融合
解决方案：
增加预融合逻辑，直到所有景点均不能够融合，开始进行新增判定
'''
    # 预融合，只有景点需要预融合，代码与融合完全相同，只不过只更新融合用的 similar_dict，但是不进行融合
    if poi_type == 'attr':
        pre_merge_times = 0

        while True:
            pre_merge_finished = True
            for source, sid, keys in get_data(cid_or_geohash=cid_or_geohash):
                start = time.time()

                # 遍历当前城市下全量的融合 key 获取 uid
                def get_uid():
                    for each_uid, similar_keys in similar_dict.items():
                        for each_key in keys:
                            if each_key in similar_keys:
                                return each_uid
                    if source == 'online':
                        return sid
                    return None

                uid = get_uid()
                if uid:
                    # 预融合未完成的标志，当前有 key 不在 similar_dict 中
                    for k in keys:
                        if k not in similar_dict[uid]:
                            pre_merge_finished = False
                    # 更新相似判断字典
                    similar_dict[uid].update(keys)

                logger.info(
                    "[pre_merge][city: {}][id: {}][pre_merge_times: {}][takes: {}]".format(cid_or_geohash, uid,
                                                                                           pre_merge_times,
                                                                                           time.time() - start))

            # 更新状态位
            pre_merge_times += 1
            if pre_merge_finished:
                # 当完成预融合，结束全部预融合操作
                break

    # 使用此中方式可以每次增量全量融合，可避免融合中出现生成 id 问题
    for source, sid, keys in get_data(cid_or_geohash=cid_or_geohash):
        start = time.time()

        # 遍历当前城市下全量的融合 key 获取 uid
        def get_uid():
            for each_uid, similar_keys in similar_dict.items():
                for each_key in keys:
                    if each_key in similar_keys:
                        if poi_type == 'attr':
                            return each_uid
                        else:
                            # 购物，餐厅同源不融合逻辑
                            _res = merged_dict.get(each_uid, None)
                            if _res:
                                for _r in _res:
                                    if source not in _r:
                                        return each_uid

            return None

        uid = get_uid()

        if not uid:
            # 当线上环境时，没有获取 uid，使用自身当前 uid，否则使用其他源的 uid
            if source == 'online':
                uid = sid
            else:
                uid = get_max_uid()

        # 更新相似判断字典
        similar_dict[uid].update(keys)
        # 更新融合内容字典
        merged_dict[uid].add((source, sid))
        # 更新白名单字典
        for _each in white_list:
            if (source, sid) in _each:
                # 如果 source，sid 在白名单项目中，融合
                for _s, _sid in _each:
                    if _s == 'qyer_url_id':
                        continue
                    merged_dict[uid].add((_s, _sid))
                    logger.info("[white list merge][uid: {}][source: {}][sid: {}]".format(uid, source, sid))

            for _each_key in keys:
                if _each_key in _each:
                    # 如果融合 key 在白名单项目中，融合
                    for _s, _sid in _each:
                        if _s == 'qyer_url_id':
                            continue
                        merged_dict[uid].add((_s, _sid))
                        logger.info("[white list merge][uid: {}][source: {}][sid: {}]".format(uid, source, sid))
                if _each_key[0] == 'qyer' and ('qyer_url_id', _each_key[1]) in _each:
                    # 如果融合 key 在白名单项目中，融合
                    for _s, _sid in _each:
                        if _s == 'qyer_url_id':
                            continue
                        merged_dict[uid].add((_s, _sid))
                        logger.info("[white list merge][uid: {}][source: {}][sid: {}]".format(uid, source, sid))

        logger.info("[finish][city: {}][id: {}][takes: {}]".format(cid_or_geohash, uid, time.time() - start))
    return merged_dict


def poi_merge(cid_or_geohash, poi_type):
    global online_table_name
    global data_source_table
    # 初始化融合需要的数据表名等
    init_global_name(poi_type)
    # 初始化白名单信息
    init_white_list()
    # 获取融合后的信息
    merged_dict = _poi_merge(cid_or_geohash)

    # 融合入各 poi unid 表
    insert_poi_unid(merged_dict, cid_or_geohash)
    update_already_merge_city(poi_type, cid_or_geohash)


if __name__ == '__main__':
    cids = [40050, 40051, 40052, 51516, 51517, 51518, 40053, 51519, 51520, 51521, 51522, 20371]
    for city_id in cids:
        poi_merge(city_id, 'attr')
