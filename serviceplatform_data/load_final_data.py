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
    "hotel_detail": "update_time",
    "attr_detail": "utime",
    "rest_detail": "utime",
    "total_detail": "insert_time",
    "hotel_images": "update_date",
    "poi_images": "date",
}

all_seek_dict = None


def init_all_seek_dict():
    local_conn = base_data_final_pool.connection()
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
    local_conn = base_data_final_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''REPLACE INTO data_insert_seek VALUES (%s, %s);''', (table_name, update_time))
    logger.debug("[update seek table][table_name: {}][update_time: {}]".format(table_name, update_time))
    local_cursor.close()
    local_conn.close()


def create_table():
    final_conn = base_data_final_pool.connection()
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
            filter(lambda x: len(x.split('_')) in (3, 4),
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-1]
        )
    )
    local_cursor.close()

    for each_table in table_list:
        each_table_key_list = each_table.split('_')
        if len(each_table_key_list) == 3:
            if each_table_key_list[0] not in ('attr', 'rest', 'total', 'hotel'):
                logger.debug('[skip table][name: {}]'.format(each_table))
                continue
            if each_table_key_list[-1] < '20170929a':
                logger.debug('[skip table][name: {}]'.format(each_table))
                continue

            # 通过表明成获取类型以及 tag
            try:
                _type, _, _tag = each_table.split('_')
            except Exception:
                logger.error('[Unknown View Final: {}]'.format(each_table))
                continue

            # 生成如数据表名
            to_table_name = "{}_final".format(_type)
            _type = "{}_detail".format(_type)
        elif len(each_table_key_list) == 4:
            if each_table_key_list[1] != 'images':
                logger.debug('[skip table][name: {}]'.format(each_table))
                continue
            try:
                _type, _, _, _tag = each_table.split('_')
            except Exception:
                logger.error('[Unknown View Final: {}]'.format(each_table))
                continue
            # 生成如数据表名
            if _type == 'hotel':
                to_table_name = "hotel_images"
            elif _type == 'poi':
                to_table_name = "poi_images"
            else:
                raise TypeError('Unknown Type: {}'.format(_type))

            _type = "{}_images".format(_type)
        else:
            continue

        u_time = get_seek(table_name=each_table)
        start = time.time()

        # 开始进行数据合并
        local_cursor = local_conn.cursor()
        if u_time == '':
            update_time_sql = '''SELECT {0}
                            FROM {1}
                            ORDER BY {0}
                            LIMIT {2};'''.format(time_key[_type], each_table, limit)
        else:
            update_time_sql = '''SELECT {0}
            FROM {1}
            WHERE {0} >= '{2}'
            ORDER BY {0}
            LIMIT {3};'''.format(time_key[_type], each_table, u_time, limit)
        line_count = local_cursor.execute(update_time_sql)

        if line_count == 0:
            # 如果已无数据，则不需要执行后面的处理
            continue
        # get final update time for inserting db next time
        final_update_time = max(map(lambda x: x[0], local_cursor.fetchall()))
        local_cursor.close()

        # replace into final data
        local_cursor = local_conn.cursor()
        if to_table_name == 'hotel_images':
            query_sql = '''REPLACE INTO {0}.{1}
            (file_name, source, sid, url, pic_size, bucket_name, url_md5, pic_md5, `use`, part, date)
              SELECT
                file_name,
                source,
                sid,
                url,
                pic_size,
                bucket_name,
                url_md5,
                pic_md5,
                `use`,
                part,
                date
              FROM
                {2} where id > {3} ORDER BY id LIMIT {4};;'''.format(final_database, to_table_name, each_table_final,
                                                                     seek, limit)
        elif to_table_name == 'poi_images':
            pass
        elif u_time != '':
            query_sql = '''REPLACE INTO {1}.{2} SELECT *
            FROM {3}
            WHERE {0} >= '{4}'
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

        # todo update final update time
    local_conn.close()


def main():
    create_table()
    load_data(limit=2000)


if __name__ == '__main__':
    main()
