#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午8:21
# @Author  : Hou Rong
# @Site    : 
# @File    : delete_zero_map_info.py
# @Software: PyCharm
from service_platform_conn_pool import service_platform_pool, fetchall


def is_map_info_legal(map_info):
    try:
        lon, lat = map_info.split(',')
        float(lon)
        float(lat)
        if lon == 0 and lat == 0:
            raise Exception()
        return True
    except Exception:
        print("[map info illegal][map_info: {}]".format(map_info))
        return False


def get_task():
    _sid_set = set()
    _sql = '''SELECT
  id,
  map_info
FROM detail_total_qyer_20171209a;'''
    for _sid, map_info in fetchall(service_platform_pool, sql=_sql):
        if is_map_info_legal(map_info=map_info):
            continue
        _sid_set.add(_sid)

    print(_sid_set)


if __name__ == '__main__':
    get_task()
