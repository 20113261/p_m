#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import poi_ori_pool, poi_face_detect_pool, service_platform_pool, base_data_final_pool
from logger import get_logger

logger = get_logger("select_and_update_table")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}


def update_sql(data):
    sql = '''UPDATE hotel_images
SET info = JSON_SET(CASE WHEN info IS NULL
  THEN '{{}}'
                    ELSE info END, '$.down_reason', '扫库 md5 不对应，过滤')
WHERE pic_md5 IN ({});'''.format(
        ','.join(
            map(lambda x: "'{}'".format(x), data)
        )
    )
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    try:
        _res = cursor.execute(sql)
    except Exception:
        print(sql)
        raise Exception()
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[total: {}][execute: {}]".format(len(data), _res))


def get_task():
    sql = '''SELECT file_name
FROM error_f_md5_file;'''
    data = []
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        _count += 1
        data.append(line[0])
        if len(data) == 1000:
            logger.info("[count: {}]".format(_count))
            update_sql(data)
            data = []
    update_sql(data)


if __name__ == '__main__':
    get_task()
