#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/31 下午3:38
# @Author  : Hou Rong
# @Site    :
# @File    : add_task_from_hotel_unid.py
# @Software: PyCharm
import pymysql
from warnings import filterwarnings
from service_platform_conn_pool import base_data_pool, fetchall, service_platform_pool
from my_logger import get_logger

filterwarnings('ignore', category=pymysql.err.Warning)

logger = get_logger("add_task_from_hotel_unid")


def get_task_info(source, sid_set):
    query_sql = '''SELECT source, sid, hotel_url
FROM hotel_unid
WHERE source = '{}' AND sid IN ({});'''.format(
        source,
        ','.join(map(lambda x: "'{}'".format(x), sid_set)
                 )
    )
    return list(fetchall(base_data_pool, query_sql))


def insert_db(table_name, data):
    sql = '''REPLACE INTO {} (source, source_id, city_id, country_id, hotel_url) VALUES (%s, %s, 'NULL', 'NULL', %s)'''.format(
        table_name)
    conn = service_platform_pool.connection()
    cursor = conn.cursor()
    res = cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[add task][table: {}][count: {}][insert: {}]".format(table_name, len(data), res))


def get_sid_set(source, sid_file_name, table_name):
    f = open('/tmp/{}'.format(sid_file_name))
    sid_set = set()
    count = 0
    for line in f:
        count += 1
        if count % 10000 == 0:
            logger.info(
                "[source: {}][sid_file_name: {}][table_name: {}][count: {}]".format(source, sid_file_name, table_name,
                                                                                    count))
        sid = line.strip()
        sid_set.add(sid)
        if len(sid_set) == 2000:
            task_info = get_task_info(source, sid_set)
            insert_db(table_name, task_info)
            sid_set = set()
    if sid_set:
        task_info = get_task_info(source, sid_set)
        insert_db(table_name, task_info)
    logger.info(
        "[source: {}][sid_file_name: {}][table_name: {}][count: {}]".format(source, sid_file_name, table_name,
                                                                            count))


if __name__ == '__main__':
    '''
lost_sid_agoda_20171127a.txt
lost_sid_booking_20171127a.txt
lost_sid_ctrip_20171127a.txt
lost_sid_elong_20171127a.txt
lost_sid_elong_20171219a.txt
lost_sid_expedia_20171127a.txt
lost_sid_hotels_20171127a.txt
    '''
    task_list = [
        # ('agoda', 'lost_sid_agoda_20171127a.txt', 'list_hotel_agoda_20171127a'),
        # ('booking', 'lost_sid_booking_20171127a.txt', 'list_hotel_booking_20171127a'),
        # ('ctrip', 'lost_sid_ctrip_20171127a.txt', 'list_hotel_ctrip_20171127a'),
        # ('elong', 'lost_sid_elong_20171127a.txt', 'list_hotel_elong_20171127a'),
        # ('expedia', 'lost_sid_expedia_20171127a.txt', 'list_hotel_expedia_20171127a'),
        # ('hotels', 'lost_sid_hotels_20171127a.txt', 'list_hotel_hotels_20171127a'),
    ]
    for source, file_name, table in task_list:
        get_sid_set(
            source=source,
            sid_file_name=file_name,
            table_name=table
        )
