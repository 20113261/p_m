#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/9 上午10:39
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_final_data.py
# @Software: PyCharm
import pymysql
import logging
import time
import pymysql.err
from logging import getLogger, StreamHandler, FileHandler
from warnings import filterwarnings
from service_platform_conn_pool import service_platform_pool

# ignore pymysql warnings
filterwarnings('ignore', category=pymysql.err.Warning)

logger = getLogger("insert_data")
logger.level = logging.DEBUG
s_handler = StreamHandler()
f_handler = FileHandler(
    filename='/search/log/cron/insert_data.log'
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

# local_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
#                              db='ServicePlatform')

all_seek_dict = None


def create_table(view_type, view_tag):
    final_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
                                 db=final_database)
    final_cursor = final_conn.cursor()
    sql_name = final_table.get(view_type, None)
    if sql_name is None:
        raise TypeError("[Unknown View Type: {}]".format(view_type))
    final_sql = open(
        '/search/hourong/PycharmProjects/PoiCommonScript/serviceplatform_data/sql/{}'.format(sql_name)).read()
    table_name = "{}_final_{}".format(view_type, view_tag)
    final_cursor.execute(final_sql % (table_name,))
    logger.debug('[create table][name: {}]'.format(table_name))
    final_cursor.close()
    final_conn.close()


def init_all_seek_dict():
    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT *
FROM data_insert_seek;''')
    global all_seek_dict
    all_seek_dict = {k: v for k, v in local_cursor.fetchall()}
    local_cursor.close()
    local_conn.close()


def get_seek(table_name):
    global all_seek_dict
    if all_seek_dict is None:
        init_all_seek_dict()
    return all_seek_dict.get(table_name, '')


def update_seek_table(table_name, update_time):
    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''REPLACE INTO data_insert_seek VALUES (%s, %s);''', (table_name, update_time))
    logger.debug("[update seek table][table_name: {}][update_time: {}]".format(table_name, update_time))
    local_conn.commit()
    local_cursor.close()
    local_conn.close()


def insert_data(limit=1000):
    logger.debug("start insert data")
    logger.debug("get all view name")

    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.VIEWS
    WHERE TABLE_SCHEMA = 'ServicePlatform';''')

    # 强制要求按照 tag 的先后顺序排列
    view_list = list(
        sorted(
            filter(lambda x: x.startswith('view_final_'),
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-1]
        )
    )
    local_cursor.close()

    for each_view_final in view_list:
        start = time.time()
        u_time = get_seek(each_view_final)

        try:
            _, _, view_type, view_source, view_tag = each_view_final.split('_')
        except Exception:
            logger.error('[Unknown View Final: {}]'.format(each_view_final))
            continue

        create_table(view_type, view_tag)
        to_table_name = "{}_final_{}".format(view_type, view_tag)
        if view_type in ('hotel', 'attr', 'rest', 'total'):
            local_cursor = local_conn.cursor()
            if u_time == '':
                update_time_sql = '''SELECT {0}
                    FROM {1}
                    ORDER BY {0}
                    LIMIT {2};'''.format(time_key[view_type], each_view_final, limit)
            else:
                update_time_sql = '''SELECT {0}
    FROM {1}
    WHERE {0} >= '{2}'
    ORDER BY {0}
    LIMIT {3};'''.format(time_key[view_type], each_view_final, u_time, limit)
            line_count = local_cursor.execute(update_time_sql)
            if line_count == 0:
                continue
            # get final update time for inserting db next time
            final_update_time = max(map(lambda x: x[0], local_cursor.fetchall()))
            local_cursor.close()

            # replace into final data
            local_cursor = local_conn.cursor()
            if u_time != '':
                query_sql = '''REPLACE INTO {1}.{2} SELECT *
    FROM {3}
    WHERE {0} >= '{4}'
    ORDER BY {0}
    LIMIT {5};'''.format(time_key[view_type], final_database, to_table_name, each_view_final, u_time, limit)
            else:
                query_sql = '''REPLACE INTO {1}.{2} SELECT *
                    FROM {3}
                    ORDER BY {0}
                    LIMIT {4};'''.format(time_key[view_type], final_database, to_table_name, each_view_final, limit)

            try:
                replace_count = local_cursor.execute(query_sql)
            except Exception as e:
                logger.exception(msg="[table_name: {}][error_sql: {}]".format(each_view_final, query_sql), exc_info=e)
                continue
            local_conn.commit()
            local_cursor.close()
        else:
            raise TypeError("Unknown Type: {}".format(view_type))

        update_seek_table(each_view_final, final_update_time)
        logger.debug(
            "[insert data][to: {}][from: {}][update_time: {}][final_update_time: {}][limit: {}][line_count: {}]["
            "replace_count: {}][takes: {}]".format(
                to_table_name,
                each_view_final,
                u_time,
                final_update_time,
                limit,
                line_count,
                replace_count,
                time.time() - start
            ))
    local_conn.close()


if __name__ == '__main__':
    insert_data(limit=5000)
