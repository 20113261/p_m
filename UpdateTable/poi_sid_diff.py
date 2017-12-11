#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午10:26
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_sid_diff.py
# @Software: PyCharm
from service_platform_conn_pool import service_platform_pool, fetchall


def get_a_set():
    sql = '''SELECT id
      FROM merged_total_qyer_1209a
      WHERE city_id != 'NULL';'''
    _set = set()
    for line in fetchall(service_platform_pool, sql=sql):
        _set.add(line[0])
    return _set


def get_b_set():
    sql = '''SELECT id
      FROM poi_merge.attr
      WHERE source = 'qyer' AND city_id != 'NULL';'''
    _set = set()
    for line in fetchall(service_platform_pool, sql=sql):
        _set.add(line[0])
    return _set


if __name__ == '__main__':
    a_set = get_a_set()
    b_set = get_b_set()
    print(a_set - b_set)
    print(b_set - a_set)
