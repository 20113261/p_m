#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/13 下午7:07
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_ori.py
# @Software: PyCharm
import time
import pymysql
import redis
from collections import defaultdict
from toolbox.Common import is_legal
from data_source import MysqlSource
from service_platform_conn_pool import base_data_pool, poi_ori_pool
from logger import get_logger

poi_type = None
online_table_name = None
data_source_table = None

max_id = None
logger = get_logger("poi_ori")


def init_global_name(_poi_type):
    global poi_type
    global online_table_name
    global data_source_table
    poi_type = 'attr'
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
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT max(id) FROM {};'''.format(online_table_name))
    _id = cursor.fetchone()[0]
    conn.close()
    return _id


def get_max_uid():
    # todo add func get max uid
    # todo each type attr rest shop
    # todo insert id into redis
    # mk final
    global max_id
    if not max_id:
        max_id = get_max_id()
    max_id = 'v' + str(int(max_id[1:]) + 1)
    return max_id


def get_data(cid_or_geohash):
    """
    返回各优先级数据，用于融合时使用，按照 线上数据 > 各源数据 ( 暂时无源内部的排序 ) 的提取规则由先向后进行数据提取
    source = ''
    sid = ''
    keys = set()
    yield source, sid, keys
    """

    # 线上数据，按照优先级排序，先进入的数据优先使用 id
    sql = '''SELECT id, name, name_en, alias
FROM {1}
WHERE city_id = {0}
ORDER BY status_online DESC, status_test DESC, official DESC, grade;'''.format(cid_or_geohash, online_table_name)
    for data in MysqlSource(onlinedb, table_or_query=sql, size=10000, is_table=False):
        keys = set()
        if is_legal(data[1]):
            keys.add(data[1])
        if is_legal(data[2]):
            keys.add(data[2])
        for key in data[3].split('|'):
            if is_legal(key):
                keys.add(key)
        logger.debug("[source: {}][sid: {}][keys: {}]".format('online', data[0], keys))
        yield 'online', data[0], keys

    # 各源数据，暂时不增加排序规则
    sql = '''SELECT
  id,
  source,
  name,
  name_en
FROM {1}
WHERE city_id = {0};'''.format(cid_or_geohash, data_source_table)
    for data in MysqlSource(data_db, table_or_query=sql, size=10000, is_table=False):
        keys = set()
        if is_legal(data[2]):
            keys.add(data[2])
        if is_legal(data[3]):
            keys.add(data[3])
        logger.debug("[source: {}][sid: {}][keys: {}]".format(data[1], data[0], keys))
        yield data[1], data[0], keys


def insert_poi_unid(merged_dict, cid_or_geohash):
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
    # # init id list
    # online_ids = set()
    # data_ids = set()
    # for _, s_sid_set in merged_dict.items():
    #     for source, sid in s_sid_set:
    #         if source == 'online':
    #             online_ids.add(sid)
    #         else:
    #             data_ids.add((source, sid))

    # get data total
    #     # get online data name name_en map_info grade star ranking address url
    #     total_data = {}
    #     _dev_conn = base_data_pool.connection()
    #     _dev_cursor = _dev_conn.cursor()
    #     _dev_cursor.execute('''SELECT
    #   id,
    #   name,
    #   name_en,
    #   map_info,
    #   grade,
    #   -1,
    #   ranking,
    #   address,
    #   ''
    # FROM chat_attraction WHERE id in ({})'''.format(','.join(map(lambda x: "'{}'".format(x), online_ids))))
    #     for line in _dev_cursor.fetchall():
    #         total_data[('online', line[0])] = line[1:]
    #     _dev_cursor.close()
    #     _dev_conn.close()
    #
    #     # todo get poi name name_en map_info grade star ranking address url
    #     _data_conn = poi_ori_pool.connection()
    #     _data_cursor = _data_conn.cursor()
    #     _data_cursor.execute('''SELECT
    #   source,
    #   id,
    #   name,
    #   name_en,
    #   map_info,
    #   grade,
    #   star,
    #   ranking,
    #   address,
    #   url
    # FROM attr
    # WHERE (source, id) IN
    #       ({});'''.format(','.join(map(lambda x: "('{}','{}')".format(x[0], x[1]), data_ids))))
    #     for line in _data_cursor.fetchall():
    #         total_data[(line[0], line[1])] = line[2:]
    #     _data_cursor.close()
    #     _data_conn.close()
    #
    #     for uid, s_sid_set in merged_dict.items():
    #         for source, sid in s_sid_set:
    #             name, name_en, map_info, grade, star, ranking, address, url = total_data[(source, sid)]

    data = []
    _dev_conn = base_data_pool.connection()
    _dev_cursor = _dev_conn.cursor()
    _data_conn = poi_ori_pool.connection()
    _data_cursor = _data_conn.cursor()
    for uid, s_sid_set in merged_dict.items():
        for source, sid in s_sid_set:
            if source == 'online':
                _dev_cursor.execute('''SELECT
  name,
  name_en,
  map_info,
  grade,
  -1,
  ranking,
  address,
  ''
FROM chat_attraction
WHERE id = '{}';'''.format(sid))
                try:
                    name, name_en, map_info, grade, star, ranking, address, url = _dev_cursor.fetchone()
                except Exception as exc:
                    logger.exception("[error sql query][source: {}][sid: {}]".format(source, sid), exc_info=exc)
                    continue
            else:
                _data_cursor.execute('''SELECT
  name,
  name_en,
  map_info,
  CASE WHEN grade != 'NULL'
    THEN grade
  ELSE -1.0 END AS grade,
  star,
  ranking,
  address,
  url
FROM attr
WHERE source = '{}' AND id = '{}';'''.format(source, sid))
                try:
                    name, name_en, map_info, grade, star, ranking, address, url = _data_cursor.fetchone()
                except Exception as exc:
                    logger.exception("[error sql query][source: {}][sid: {}]".format(source, sid), exc_info=exc)
                    continue

            data.append((uid, cid, city_name, country, city_map_info, source, sid, name, name_en, map_info, grade,
                         star, ranking, address, url))
    _dev_cursor.close()
    _data_cursor.close()
    _dev_conn.close()
    _data_conn.close()

    _final_conn = poi_ori_pool.connection()
    _final_cursor = _final_conn.cursor()
    for d in data:
        try:
            _final_cursor.execute(
                '''REPLACE INTO attr_unid (id, city_id, city_name, country_name, city_map_info, source, source_id, name, name_en, map_info, grade, star, ranking, address, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''',
                d
            )
        except Exception as exc:
            logger.exception("[insert unid table error][data: {}]".format(d), exc_info=exc)
    _final_conn.commit()
    _final_cursor.close()
    _final_conn.close()

    # logger.debug("[finish prepare data][city: {}][replace_count: {}][takes: {}]".format(cid_or_geohash, line_count,
    #                                                                                     time.time() - start))


def _poi_merge(cid_or_geohash):
    # 生成空的用于融合的相似字典存放数据
    similar_dict = defaultdict(set)
    merged_dict = defaultdict(set)

    # 使用此中方式可以每次增量全量融合，可避免融合中出现生成 id 问题
    for source, sid, keys in get_data(cid_or_geohash=cid_or_geohash):
        start = time.time()

        # 遍历当前城市下全量的融合 key 获取 uid
        def get_uid():
            for each_uid, similar_keys in similar_dict.items():
                for each_key in keys:
                    if each_key in similar_keys:
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
        logger.debug("[finish][city: {}][id: {}][takes: {}]".format(cid_or_geohash, uid, time.time() - start))
    return merged_dict


def poi_merge(cid_or_geohash):
    # 初始化融合需要的数据表名等
    # todo 修改为可变动的内容
    init_global_name('attr')
    # 获取融合后的信息
    merged_dict = _poi_merge(cid_or_geohash)

    # todo 融合入各 poi unid 表
    insert_poi_unid(merged_dict, cid_or_geohash)
    for k, v in merged_dict.items():
        if len(v) > 1:
            logger.debug("[union_info][uid: {}][each_keys: {}]".format(k, v))


if __name__ == '__main__':
    poi_merge(10001)
