#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/16 下午3:18
# @Author  : Hou Rong
# @Site    : 
# @File    : multi_city.py
# @Software: PyCharm
import gevent.monkey

gevent.monkey.patch_all()
import time
import gevent.pool
from logger import get_logger
from service_platform_conn_pool import base_data_pool
from poi_ori.poi_merge import poi_merge
from poi_ori.already_merged_city import init_already_merged_city

logger = get_logger("multi_city")
pool = gevent.pool.Pool(size=50)


def poi_ori(poi_type):
    already_merged_city = init_already_merged_city(poi_type=poi_type)
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT id
FROM city;''')
    cids = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()
    conn.close()
    for cid in cids:
        if cid in already_merged_city:
            continue
        start = time.time()
        logger.info('[start][cid: {}]'.format(cid))
        pool.apply_async(poi_merge, args=(cid, poi_type))
        logger.info('[end][cid: {}][takes: {}]'.format(cid, time.time() - start))
    pool.join()


if __name__ == '__main__':
    import sys

    _t = sys.argv[1]
    if _t in ('attr', 'shop'):
        poi_ori(_t)
    else:
        print("python3 multi_city.py attr/shop")
