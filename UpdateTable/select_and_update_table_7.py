#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import spider_data_base_data_pool
from logger import get_logger
from toolbox.Hash import encode

logger = get_logger("select_and_update_table")

base_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


def update_sql(data):
    sql = '''UPDATE hotel_0110_new
SET first_img = %s, img_list = %s
WHERE uid = %s;'''
    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    try:
        _res = cursor.executemany(sql, data)
    except Exception:
        print(sql)
        raise Exception()
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[total: {}][execute: {}]".format(len(data), _res))


def get_task():
    sql = '''SELECT
  uid,
  first_img,
  img_list
FROM hotel;'''
    data = []
    _count = 0
    for line in MysqlSource(base_data_config, table_or_query=sql,
                            size=2000, is_table=False,
                            is_dict_cursor=False):
        _count += 1
        data.append((line[1], line[2], line[0]))
        if len(data) == 2000:
            logger.info("[count: {}]".format(_count))
            update_sql(data)
            data = []
    update_sql(data)


if __name__ == '__main__':
    get_task()
