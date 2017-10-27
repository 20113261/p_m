#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午3:15
# @Author  : Hou Rong
# @Site    : 
# @File    : update_tag_id.py
# @Software: PyCharm
from toolbox.Common import is_legal
from service_platform_conn_pool import poi_ori_pool, base_data_pool
from logger import get_logger
from collections import defaultdict

logger = get_logger("update_tag_id")

poi_type = None
tag_s = None
tag_b = None
task_table = None
id2tag = {}


def init_global_name(_poi_type):
    global poi_type
    global tag_s
    global tag_b
    global task_table
    global id2tag
    poi_type = _poi_type
    if poi_type == 'attr':
        tag_s = 'chat_attraction_tagS'
        tag_b = 'chat_attraction_tagB'
        task_table = 'chat_attraction'
    elif poi_type == 'shop':
        tag_s = 'chat_shopping_tagS'
        tag_b = 'chat_attraction_tagB'
        task_table = 'chat_shopping'
    elif poi_type == 'rest':
        tag_s = 'chat_restaurant_tagS'
        tag_b = 'chat_attraction_tagB'
        task_table = 'chat_restaurant'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))

    id2tag = init_id2tag()


def tag2id():
    conn = base_data_pool.connection()
    sql = '''SELECT
  id,
  tag
FROM {};'''.format(tag_s)
    cursor = conn.cursor()
    cursor.execute(sql)
    _dict = {}
    for _id, _tag_name in cursor.fetchall():
        _dict[_tag_name] = str(_id)
    cursor.close()
    conn.close()
    return _dict


def init_id2tag():
    conn = base_data_pool.connection()
    sql = '''SELECT
  id,
  Stag
FROM {};'''.format(tag_b)
    cursor = conn.cursor()
    cursor.execute(sql)
    _dict = defaultdict(set)
    for _id, _l_s_tag in cursor.fetchall():
        for each in _l_s_tag.split('|'):
            if is_legal(each):
                _dict[_id].add(each)
    cursor.close()
    conn.close()
    return _dict


def get_tag(tag_id):
    b_tag_set = set()
    for each in tag_id.split('|'):
        for b_tag, s_tags in id2tag.items():
            if each in s_tags:
                b_tag_set.add(int(b_tag[2:]))

    _l_tag = []
    if poi_type == 'attr':
        _tag_length = 10
    elif poi_type == 'shop':
        _tag_length = 3
    for i in range(_tag_length):
        if i in b_tag_set:
            _l_tag.append('1')
        else:
            _l_tag.append('0')
    return ''.join(_l_tag)


def update_each_tag_id():
    tag_id = tag2id()
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT id,norm_tagid
FROM {};'''.format(
        task_table))
    data = []
    _count = 0
    for _id, _tag_id in cursor.fetchall():
        if is_legal(_tag_id):
            tag_id_set = set()
            for each in _tag_id.split('|'):
                tag_id_set.add(tag_id.get(each))
            small_tag = ('|'.join(filter(lambda x: is_legal(x), tag_id_set)))
            big_tag = get_tag(small_tag)
            data.append((small_tag, big_tag, _id))
            _count += 1
            if len(data) % 1000 == 0:
                logger.debug("[mk data][poi_type: {}][len: {}]".format(poi_type, _count))
                res = cursor.executemany('update base_data.{} set tag_id=%s, tagB=%s where id=%s'.format(task_table),
                                         data)
                data = []
                logger.debug("[update tag id][table_name: {}][update count: {}]".format(task_table, res))

    logger.debug("[mk data finished][poi_type: {}][len: {}]".format(poi_type, _count))
    conn.commit()
    cursor.close()
    conn.close()


def update_tag_id(_poi_type):
    init_global_name(_poi_type)
    update_each_tag_id()


if __name__ == '__main__':
    update_tag_id('shop')
