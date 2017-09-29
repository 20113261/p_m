#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/26 下午1:18
# @Author  : Hou Rong
# @Site    : 
# @File    : dump_detail_csv.py
# @Software: PyCharm
import pandas
import pymysql
import time
import logging
from logging import getLogger, StreamHandler
from sqlalchemy.engine import create_engine

logger = getLogger("dump_detail_csv")
logger.level = logging.DEBUG
s_handler = StreamHandler()
logger.addHandler(s_handler)

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
        filter(lambda x: not x.endswith('test') and x.startswith('view_') and not x.startswith('view_final_'),
               table_list))
    for table in table_name:
        if 'hotel' in table:
            # 跳过 hotel 类型
            continue
        start = time.time()
        logger.debug("start dump case csv : {0}".format(table))
        engine = create_engine('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/ServicePlatform?charset=utf8')
        pandas.read_sql('SELECT * FROM {} ORDER BY rand() LIMIT 100;'.format(table), engine).to_csv(
            '/tmp/crawled_data/{}.csv'.format(table))
        logger.debug("dump table {0} takes time: {1}".format(table, time.time() - start))
