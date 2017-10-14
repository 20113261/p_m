#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/9 下午8:25
# @Author  : Hou Rong
# @Site    : 
# @File    : load_final_data.py
# @Software: PyCharm
import pymysql
import logging
import time
import pymysql.err
from logging import getLogger, StreamHandler, FileHandler
from warnings import filterwarnings
from service_platform_conn_pool import base_data_final_pool

# ignore pymysql warnings
filterwarnings('ignore', category=pymysql.err.Warning)

logger = getLogger("load_data")
logger.level = logging.DEBUG
s_handler = StreamHandler()
f_handler = FileHandler(
    filename='/search/log/cron/load_data.log'
)
logger.addHandler(s_handler)
logger.addHandler(f_handler)

final_database = 'BaseDataFinal'

final_table = {
    "hotel": "hotel_detail.sql",
    "attr": "daodao_attr_detail.sql",
    "rest": "daodao_rest_detail.sql",
    "total": "qyer_detail.sql"
}

time_key = {
    "hotel": "update_time",
    "attr": "utime",
    "rest": "utime",
    "total": "insert_time"
}


def create_table():
    final_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
                                 db=final_database)
    final_cursor = final_conn.cursor()
    for k, v in final_table.items():
        final_sql = open('/search/hourong/PycharmProjects/PoiCommonScript/serviceplatform_data/sql/{}'.format(v)).read()
        table_name = "{}_final".format(k)
        final_cursor.execute(final_sql % (table_name,))
        logger.debug('[create table][name: {}]'.format(table_name))
    final_cursor.close()
    final_conn.close()


def load_data(limit=400):
    local_conn = base_data_final_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '{}';'''.format(final_database))

    # 强制要求按照 tag 的先后顺序排列
    table_list = list(
        sorted(
            filter(lambda x: len(x.split('_')) == 3,
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-1]
        )
    )
    local_cursor.close()

    for each_table in table_list:
        if each_table.split('_')[-1] < '20170929a':
            logger.debug('[skip table][name: {}]'.format(each_table))
            continue
        u_time = ''
        finished = False

        try:
            _type, _, _tag = each_table.split('_')
        except Exception:
            logger.error('[Unknown View Final: {}]'.format(each_table))
            continue

        while not finished:
            start = time.time()
            to_table_name = "{}_final".format(_type)
            local_cursor = local_conn.cursor()
            if u_time == '':
                update_time_sql = '''SELECT {0}
                                FROM {1}
                                ORDER BY {0}
                                LIMIT {2};'''.format(time_key[_type], each_table, limit)
            else:
                update_time_sql = '''SELECT {0}
                FROM {1}
                WHERE {0} > '{2}'
                ORDER BY {0}
                LIMIT {3};'''.format(time_key[_type], each_table, u_time, limit)
            line_count = local_cursor.execute(update_time_sql)

            if line_count == 0:
                finished = True
                continue
            # get final update time for inserting db next time
            final_update_time = max(map(lambda x: x[0], local_cursor.fetchall()))
            local_cursor.close()

            # replace into final data
            local_cursor = local_conn.cursor()
            if u_time != '':
                query_sql = '''REPLACE INTO {1}.{2} SELECT *
                FROM {3}
                WHERE {0} > '{4}'
                ORDER BY {0}
                LIMIT {5};'''.format(time_key[_type], final_database, to_table_name, each_table, u_time, limit)
            else:
                query_sql = '''REPLACE INTO {1}.{2} SELECT *
                                FROM {3}
                                ORDER BY {0}
                                LIMIT {4};'''.format(time_key[_type], final_database, to_table_name, each_table,
                                                     limit)

            try:
                replace_count = local_cursor.execute(query_sql)
            except Exception as e:
                logger.exception(msg="[table_name: {}][error_sql: {}]".format(each_table, query_sql), exc_info=e)
                continue
            local_conn.commit()
            local_cursor.close()

            logger.debug(
                "[insert data][to: {}][from: {}][update_time: {}][final_update_time: {}][limit: {}][line_count: {}]["
                "replace_count: {}][takes: {}]".format(
                    to_table_name,
                    each_table,
                    u_time,
                    final_update_time,
                    limit,
                    line_count,
                    replace_count,
                    time.time() - start
                ))

            u_time = final_update_time
    local_conn.close()


def main():
    create_table()
    load_data(limit=2000)


if __name__ == '__main__':
    main()
