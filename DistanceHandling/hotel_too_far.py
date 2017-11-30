#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午7:20
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_too_far.py
# @Software: PyCharm
import numpy as np
from data_source import MysqlSource
from service_platform_conn_pool import base_data_pool, spider_task_tmp_pool
from logger import get_logger

logger = get_logger("hotel_too_far")

spider_task_data_config = {
    'host': '10.10.238.148',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'tmp'
}


def dist_from_coordinates(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    # conversion to radians
    d_lat = np.radians(lat2 - lat1)
    d_lon = np.radians(lon2 - lon1)

    r_lat1 = np.radians(lat1)
    r_lat2 = np.radians(lat2)

    # haversine formula
    a = np.sin(d_lat / 2.) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(d_lon / 2.) ** 2

    haversine = 2 * R * np.arcsin(np.sqrt(a))

    return haversine


def get_distance(c_map_info, p_map_info):
    try:
        c_lon, c_lat = c_map_info.split(',')
        p_lon, p_lat = p_map_info.split(',')
        return dist_from_coordinates(float(c_lat), float(c_lon), float(p_lat), float(p_lon))
    except:
        pass
    return -1


def get_c_info():
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  map_info
FROM city
WHERE map_info IS NOT NULL AND map_info != 'NULL';''')
    res = {line[0]: line[1] for line in cursor.fetchall()}
    cursor.close()
    conn.close()
    return res


def update_data(data):
    conn = spider_task_tmp_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany('''UPDATE hotel_final
SET city_id = 'NULL'
WHERE source = %s AND source_id = %s AND city_id = %s;''', data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[update too far hotel][count: {}][update: {}]".format(len(data), _res))


def main():
    c_dict = get_c_info()

    _sql = '''SELECT map_info, city_id, source, source_id
FROM hotel_final
WHERE city_id != 'NULL' AND city_id IS NOT NULL;'''

    offset = 0
    error = 0
    new_data = []

    f_res = open('/search/hourong/no_cid_hotel.sql', 'w')
    f_del = open('/search/hourong/no_cid_hotel_del.sql', 'w')

    for line in MysqlSource(db_config=spider_task_data_config,
                            table_or_query=_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        offset += 1
        _map_info = line['map_info']
        _city_id = line['city_id']
        _source = line['source']
        _source_id = line['source_id']
        _c_map_info = c_dict.get(_city_id)

        if not _c_map_info:
            continue

        dist = get_distance(_c_map_info, _map_info)

        if dist == -1:
            continue

        if get_distance(_c_map_info, _map_info) > 50:
            error += 1
            f_res.write(
                '''UPDATE IGNORE `hotel_final` SET city_id = 'NULL' WHERE source = '{}' AND source_id = '{}' AND city_id = '{}';\n'''.format(
                    _source, _source_id, _city_id))
            f_del.write(
                '''DELETE FROM hotel_final WHERE source = '{}' AND source_id = '{}' AND city_id = '{}';\n'''.format(
                    _source, _source_id, _city_id))
            # new_data.append((_source, _source_id, _city_id))
            # if len(new_data) == 2000:
            #     update_data(data=new_data)
            #     new_data = []
            logger.info(
                "[error_distance][offset: {}][error: {}][dist: {}][source: {}][source_id: {}][city_id: {}]".format(
                    offset, error, dist, _source, _source_id,
                    _city_id))

    f_res.close()
    f_del.close(x)


if __name__ == '__main__':
    main()
