#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/26 下午1:18
# @Author  : Hou Rong
# @Site    : 
# @File    : dump_detail_csv.py
# @Software: PyCharm
import pandas
import pymysql
from sqlalchemy.engine import create_engine

if __name__ == '__main__':
    local_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
                                 db='ServicePlatform')

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    table_name = list(
        filter(lambda x: x.endswith('20170925d') and x.startswith('view_') and not x.startswith('view_final_'),
               table_list))
    for table in table_name:
        print(table)
        engine = create_engine('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/ServicePlatform?charset=utf8')
        pandas.read_sql_table(table, engine).to_csv('/tmp/crawled_data/{}.csv'.format(table))
