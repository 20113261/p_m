#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import poi_ori_pool
from logger import get_logger

logger = get_logger("select_and_delete_table")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'poi_merge'
}


def update_sql(data):
    sql = '''DELETE FROM chat_attraction WHERE id=%s;'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(sql, data)
    cursor.close()
    conn.close()
    logger.info("[total: {}][execute: {}]".format(len(data), _res))


def get_task():
    sql = '''SELECT mioji_id FROM filter_data_already_online;'''
    data = []
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        data.append(line['mioji_id'])
        if len(data) == 2000:
            update_sql(data)
            data = []
    update_sql(data)


if __name__ == '__main__':
    get_task()
