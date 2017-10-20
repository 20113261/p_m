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

logger = get_logger("update_tag_id")

poi_type = None
tag_s = None
task_table = None


def init_global_name(_poi_type):
    global poi_type
    global tag_s
    global task_table
    poi_type = _poi_type
    if poi_type == 'attr':
        tag_s = 'chat_attraction_tagS'
        task_table = 'chat_attraction'
    elif poi_type == 'shop':
        tag_s = 'chat_shopping_tagS'
        task_table = 'chat_shopping'
    elif poi_type == 'rest':
        tag_s = 'chat_restaurant_tagS'
        task_table = 'chat_restaurant'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))


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


def update_each_tag_id():
    tag_id = tag2id()
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT id,norm_tagid
FROM {};'''.format(
        task_table))
    data = []
    for _id, _tag_id in cursor.fetchall():
        if is_legal(_tag_id):
            tag_id_set = set()
            for each in _tag_id.split('|'):
                tag_id_set.add(tag_id.get(each))
            data.append(('|'.join(filter(lambda x: x is not None, tag_id_set)), _id))
    res = cursor.executemany('update base_data.{} set tag_id=%s where id=%s'.format(task_table), data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.debug("[update tag id][table_name: {}][update count: {}]".format(task_table, res))


def update_tag_id(_poi_type):
    init_global_name(_poi_type)
    update_each_tag_id()


if __name__ == '__main__':
    update_tag_id('attr')
