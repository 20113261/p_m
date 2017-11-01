#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/1 下午10:47
# @Author  : Hou Rong
# @Site    : 
# @File    : mysql_json_loads.py
# @Software: PyCharm
from service_platform_conn_pool import service_platform_pool


def test():
    conn = service_platform_pool.connection()
    cursor = conn.cursor()
    query_sql = '''SELECT
  file_name,
  source,
  sid,
  info->'$.p_hash' as p_hash
FROM image_wanle_huantaoyou_20171023a;'''
    cursor.execute(query_sql)
    for line in cursor.fetchall():
        print(str(line[-1]))
    cursor.close()
    conn.close()


def update_test():
    conn = service_platform_pool.connection()
    cursor = conn.cursor()
    query_sql = '''UPDATE image_wanle_huantaoyou_20171023a
SET info = %s
WHERE file_name = %s;'''
    cursor.execute(query_sql, ({"a": "1"}, '23498847e5190ef6849a5bfcf0e506d2.png'))
    # for line in cursor.fetchall():
    #     print(str(line[-1]))
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    # test()
    update_test()
