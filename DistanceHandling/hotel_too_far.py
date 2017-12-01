#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午7:20
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_too_far.py
# @Software: PyCharm
import os
import numpy as np
from data_source import MysqlSource
from service_platform_conn_pool import base_data_pool, spider_task_tmp_pool
from logger import get_logger

logger = get_logger("hotel_too_far")

SQL_PATH = '/search/hourong/data_sql'

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


# def update_data(data):
#     conn = spider_task_tmp_pool.connection()
#     cursor = conn.cursor()
#     _res = cursor.executemany('''UPDATE hotel_final
# SET city_id = 'NULL'
# WHERE source = %s AND source_id = %s AND city_id = %s;''', data)
#     conn.commit()
#     cursor.close()
#     conn.close()
#     logger.info("[update too far hotel][count: {}][update: {}]".format(len(data), _res))

def get_sql(table_name, res_f, res_del_f, data):
    """
    :param table_name:
    :param res_f:
    :param res_del_f:
    :param data:
    :return:
    """
    # type: str, open(), open(), list
    update_sql = '''UPDATE IGNORE `{}` SET city_id = 'NULL' WHERE (source, source_id, city_id) in ({});\n'''.format(
        table_name, ",".join(map(lambda x: "('{}', '{}', '{}')".format(*x), data))
    )
    delete_sql = '''DELETE FROM `{}` WHERE (source, source_id, city_id) in ({});\n'''.format(
        table_name, ",".join(map(lambda x: "('{}', '{}', '{}')".format(*x), data))
    )
    res_f.write(update_sql)
    res_del_f.write(delete_sql)
    logger.info("[get data][count: {}]".format(len(data)))


def update_table(u_sql_name, d_sql_name):
    # execute update sql
    command = '/usr/bin/mysql -h{host} -u{user} -p{password} {db} < "{sql_path}/{sql_name}"'. \
        format(**
               {
                   'host': spider_task_data_config['host'],
                   'user': spider_task_data_config['user'],
                   'password': spider_task_data_config['password'],
                   'db': spider_task_data_config['db'],
                   'sql_path': SQL_PATH,
                   'sql_name': u_sql_name
               }
               )
    os.system(command=command)
    logger.info("[execute][command: {}]".format(command))

    # execute delete sql
    command = 'mysql -h{host} -u{user} -p{password} {db} < "{sql_path}/{sql_name}"'. \
        format(**
               {
                   'host': spider_task_data_config['host'],
                   'user': spider_task_data_config['user'],
                   'password': spider_task_data_config['password'],
                   'db': spider_task_data_config['db'],
                   'sql_path': SQL_PATH,
                   'sql_name': d_sql_name
               }
               )
    os.system(command=command)
    logger.info("[execute][command: {}]".format(command))


def detect_table(table_name):
    c_dict = get_c_info()

    _sql = '''SELECT map_info, city_id, source, source_id
FROM {}
WHERE city_id != 'NULL' AND city_id IS NOT NULL;'''.format(table_name)

    offset = 0
    error = 0
    new_data = []

    update_sql_name = 'no_cid_hotel|_|{}.sql'.format(table_name)
    del_sql_name = 'no_cid_hotel_del|_|{}.sql'.format(table_name)
    f_res = open(os.path.join(SQL_PATH, update_sql_name), 'w')
    f_del = open(os.path.join(SQL_PATH, del_sql_name), 'w')

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

            new_data.append((_source, _source_id, _city_id))
            if len(new_data) == 200:
                get_sql(table_name=table_name, res_f=f_res, res_del_f=f_del, data=new_data)
                new_data = []
            logger.info(
                "[error_distance][offset: {}][error: {}][dist: {}][source: {}][source_id: {}][city_id: {}]".format(
                    offset, error, dist, _source, _source_id,
                    _city_id))
    if new_data:
        get_sql(table_name=table_name, res_f=f_res, res_del_f=f_del, data=new_data)
    f_res.close()
    f_del.close()

    update_table(u_sql_name=update_sql_name, d_sql_name=del_sql_name)


def main():
    _sql = '''SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'tmp';'''
    for line in MysqlSource(db_config=spider_task_data_config,
                            table_or_query=_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        table_name = line[0]
        logger.info("[start][table_name: {}]".format(table_name))
        detect_table(table_name=table_name)


if __name__ == '__main__':
    main()
