#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/4 下午5:05
# @Author  : Hou Rong
# @Site    : 
# @File    : update_qyer_city_id.py
# @Software: PyCharm
import json
from service_platform_conn_pool import source_info_pool, spider_data_base_data_pool, service_platform_pool


def get_tasks():
    sql = '''SELECT
  others_info,
  city_id
FROM ota_location WHERE source='qyer';'''
    conn = source_info_pool.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    for line in cursor.fetchall():
        j_data = json.loads(line[0])
        if 's_city_id' in j_data:
            if line[1] != 'NULL':
                print('prepare', (j_data['s_city_id'], line[1]))
                yield j_data['s_city_id'], line[1]
    cursor.close()
    conn.close()


def update_db(data):
    __conn = service_platform_pool.connection()
    __cursor = __conn.cursor()
    print('start', line)
    __res = __cursor.executemany('''UPDATE new_detail_qyer_1212
    SET city_id = %s
    WHERE source_city_id = %s;''', data)
    print('end', line, len(data), __res, _count)
    __conn.commit()
    __conn.close()


if __name__ == '__main__':
    _count = 0
    data = []
    for line in list(get_tasks()):
        _count += 1
        data.append((line[1], line[0]))
        if _count % 100 == 0:
            print(_count)
            update_db(data)
            data = []
    print(_count)
    if data:
        update_db(data)
