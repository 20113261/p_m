#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/13 下午7:07
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_ori.py
# @Software: PyCharm
import logging
import time
import redis
import pymysql
from logging import getLogger, StreamHandler
from collections import defaultdict
from toolbox.Common import is_legal
from poi_ori.data_source import MysqlSource

logger = getLogger("poi_ori")
logger.level = logging.DEBUG
handler = StreamHandler()
logger.addHandler(handler)

poi_type = 'attr'
data_source_list = ['daodao', 'qyer']
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
| - city
| -  -  - poi
| -  -  -  -  merge_key set(记录全量的融合索引)
| -  -  -  -  merged_source_and_sid  set(记录原始信息的 source、sid) 
'''

_id = 0

r = redis.Redis(host='10.10.180.145')


def get_max_id():
    conn = pymysql.connect(**onlinedb)
    cursor = conn.cursor()
    cursor.execute('''SELECT max(id) FROM {};'''.format(online_table_name))
    _id = cursor.fetchone()[0]
    return _id


max_id = get_max_id()


def get_max_uid():
    # todo add func get max uid
    # todo each type attr rest shop
    # todo insert id into redis
    # mk final
    global max_id
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
    sql = '''SELECT *
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
        yield data[1], data[0], keys


def insert_poi_unid():
    pass


def _poi_merge(cid_or_geohash):
    # todo onlinedb data merge, official > 0
    # todo onlinedb data merge, official == 0
    # todo each source merge, priority: daodao > qyer
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

        merged_dict[uid].add((source, sid))
        logger.debug("[finish][city: {}][id: {}][takes: {}]".format(cid_or_geohash, uid, time.time() - start))
    return merged_dict


def poi_merge(cid_or_geohash):
    # 获取融合后的信息
    merged_dict = _poi_merge(cid_or_geohash)

    # 融合入各 poi unid 表
    insert_poi_unid()
    for k, v in merged_dict.items():
        if len(v) > 1:
            logger.debug(k, v)


if __name__ == '__main__':
    poi_merge(10001)
