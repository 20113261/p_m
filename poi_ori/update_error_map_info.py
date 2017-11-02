#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/2 下午6:13
# @Author  : Hou Rong
# @Site    : 
# @File    : update_error_map_info.py
# @Software: PyCharm
from pymysql.cursors import DictCursor
from service_platform_conn_pool import poi_ori_pool, spider_data_base_data_pool
from Common.Utils import retry
from logger import get_logger

logger = get_logger("update error map info")


@retry(times=3)
def update_db(data):
    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    logger.debug("[start update data][total: {}]".format(len(data)))
    _sql = """UPDATE chat_attraction
SET map_info = %s
WHERE id = %s;"""
    _res = cursor.executemany(_sql, data)
    logger.debug("[update data][total: {}][execute: {}]".format(len(data), _res))
    cursor.close()
    conn.close()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_id_map_info(table_name):
    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute("""SELECT
  attr_unid.id,
  attr_unid.source,
  attr_unid.source_id,
  attr_unid.map_info                                    AS old_map_info,
  ServicePlatform.{0}.map_info AS new_map_info,
  ServicePlatform.{0}.address
FROM attr_unid
  JOIN ServicePlatform.{0}
    ON attr_unid.source = ServicePlatform.{0}.source AND
       attr_unid.source_id = ServicePlatform.{0}.id
WHERE ServicePlatform.{0}.id IN (SELECT DISTINCT sid
                                                          FROM ServicePlatform.supplement_field
                                                          WHERE SOURCE = 'daodao' AND
                                                                TABLE_NAME =
                                                                '{0}');""".format(table_name))

    data = []
    for line in cursor.fetchall():
        data.append((line['new_map_info'], line['id']))
        if len(data) % 1000 == 0:
            logger.debug("[now data][count: {}]".format(len(data)))
    logger.debug("[now data][count: {}]".format(len(data)))

    for d in chunks(data, 1000):
        update_db(d)
    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_id_map_info("detail_attr_daodao_20170929a")
