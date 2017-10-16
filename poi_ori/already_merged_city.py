#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/16 下午6:36
# @Author  : Hou Rong
# @Site    : 
# @File    : already_merged_city.py
# @Software: PyCharm
from service_platform_conn_pool import poi_ori_pool


def init_already_merged_city(poi_type):
    _conn = poi_ori_pool.connection()
    _cursor = _conn.cursor()
    _cursor.execute('''SELECT * FROM already_merged_city WHERE type='{}';'''.format(poi_type))
    res = list(map(lambda x: x[1], _cursor.fetchall()))
    _cursor.close()
    _conn.close()
    return res


def update_already_merge_city(poi_type, cid):
    _conn = poi_ori_pool.connection()
    _cursor = _conn.cursor()
    res = _cursor.execute('''REPLACE INTO already_merged_city VALUES (%s,%s)''', (poi_type, cid))
    _conn.commit()
    _cursor.close()
    _conn.close()
    return res
