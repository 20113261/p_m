#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/18 下午1:14
# @Author  : Hou Rong
# @Site    : 
# @File    : init_multi_city_insert_db.py
# @Software: PyCharm
import time
import os
from my_logger import get_logger
from service_platform_conn_pool import base_data_pool
from poi_ori.already_merged_city import init_already_merged_city

logger = get_logger("init_multi_city_insert_db")


def init_task(poi_type, process_num):
    # init city_list
    city_list = [[] for i in range(process_num)]
    already_merged_city = init_already_merged_city(poi_type="{}_data".format(poi_type))
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT id
FROM city;''')
    cids = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()
    conn.close()
    _count = 0
    for cid in cids:
        if str(cid) in already_merged_city:
            continue
        _count += 1
        start = time.time()
        logger.info('[start][cid: {}]'.format(cid))
        city_list[_count % process_num].append(cid)
        logger.info('[end][cid: {}][takes: {}]'.format(cid, time.time() - start))

    for each_cids in city_list:
        os.system("nohup python3 multi_city_insert_db.py {} {} &".format(poi_type, ' '.join(each_cids)))


if __name__ == '__main__':
    init_task('attr', 6)
