#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/9 下午8:25
# @Author  : Hou Rong
# @Site    : 
# @File    : load_final_data.py
# @Software: PyCharm
import os
import pymysql
import time
import pymysql.err
from warnings import filterwarnings
from service_platform_conn_pool import base_data_final_pool
from my_logger import get_logger

# ignore pymysql warnings
filterwarnings('ignore', category=pymysql.err.Warning)

logger = get_logger("load_data")

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
    return all_seek_dict.get(table_name, '1970-1-1')


def update_seek_table(table_name, update_time):
    local_conn = base_data_final_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''REPLACE INTO data_insert_seek VALUES (%s, %s);''', (table_name, update_time))
    logger.debug("[update seek table][table_name: {}][update_time: {}]".format(table_name, update_time))
    global all_seek_dict
    all_seek_dict[table_name] = update_time
    local_conn.commit()
    local_cursor.close()
    local_conn.close()


def create_table():
    final_conn = base_data_final_pool.connection()
    final_cursor = final_conn.cursor()
    for k, v in final_table.items():
        real_path = os.path.split(os.path.realpath(__file__))[0]
        sql_path = os.path.join(real_path, 'sql', v)
        final_sql = open(sql_path).read()
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
        update_time_sql = '''SELECT {0}
        FROM {1}
        WHERE {0} >= '{2}'
        ORDER BY {0}
        LIMIT {3};'''.format(time_key[_type], each_table, u_time, limit)
        line_count = local_cursor.execute(update_time_sql)

        logger.debug('sql: %s\nselect_count: %s' % (update_time_sql, str(local_cursor.rowcount)))

        if line_count == 0:
            # 如果已无数据，则不需要执行后面的处理
            continue
        # get final update time for inserting db next time
        final_update_time = max(map(lambda x: x[0], local_cursor.fetchall()))
        logger.debug('each_table: %s  final_update_time: %s' % (each_table, str(final_update_time)))
        local_cursor.close()

        # replace into final data
        local_cursor = local_conn.cursor()
        query_sql_list = []
        if to_table_name == 'hotel_images':
            query_sql = '''REPLACE INTO {0} (source, source_id, pic_url, pic_md5, part, hotel_id, status, update_date, size, flag, file_md5)
  SELECT
    source,
    source_id,
    pic_url,
    pic_md5,
    part,
    hotel_id,
    status,
    update_date,
    size,
    flag,
    file_md5
  FROM
    {1}
  WHERE update_date >= '{2}'
  ORDER BY update_date
  LIMIT {3};'''.format(to_table_name, each_table, u_time, limit)
            query_sql_list.append(query_sql)
        elif to_table_name == 'poi_images':
            query_sql = '''REPLACE INTO {0}
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
                {1}
              WHERE date >= '{2}'
              ORDER BY date
              LIMIT {3};'''.format(to_table_name, each_table, u_time, limit)
            query_sql_list.append(query_sql)
        elif u_time != '':
            query_sql = '''REPLACE INTO {1} SELECT *
            FROM {2}
            WHERE {0} >= '{3}'
            ORDER BY {0}
            LIMIT {4};'''.format(time_key[_type], to_table_name, each_table, u_time, limit)
            query_sql_list.append(query_sql)
            if to_table_name == 'attr_final':
                query_sql = '''REPLACE INTO poi_merge.attr SELECT *
                            FROM {2}
                            WHERE {0} >= '{3}'
                            ORDER BY {0}
                            LIMIT {4};'''.format(time_key[_type], to_table_name, each_table, u_time, limit)
                query_sql_list.append(query_sql)
            elif to_table_name == 'total_final':
                query_sql = '''REPLACE INTO poi_merge.attr
  SELECT
    id,
    source,
    name,
    name_en,
    alias,
    map_info,
    city_id,
    source_city_id,
    address,
    star,
    recommend_lv,
    pv,
    plantocounts,
    beentocounts,
    overall_rank,
    ranking,
    grade,
    grade_distrib,
    commentcounts,
    tips,
    tagid,
    related_pois,
    nomissed,
    keyword,
    cateid,
    url,
    phone,
    site,
    imgurl,
    commenturl,
    introduction,
    '',
    opentime,
    price,
    recommended_time,
    wayto,
    0,
    0,
    insert_time
  FROM {2}
  WHERE {0} > '{3}'
  ORDER BY {0}
  LIMIT {4};'''.format(time_key[_type], to_table_name, each_table, u_time, limit)
                query_sql_list.append(query_sql)
                # elif to_table_name == 'rest_final':
                #     query_sql = '''REPLACE INTO poi_merge.rest SELECT *
                #                 FROM {2}
                #                 WHERE {0} >= '{3}'
                #                 ORDER BY {0}
                #                 LIMIT {4};'''.format(time_key[_type], to_table_name, each_table, u_time, limit)

        else:
            raise TypeError("Unknown Type [u_time: {}][to_table_name: {}]".format(u_time, to_table_name))

        for _each_query_sql in query_sql_list:
            is_replace = True
            try:
                replace_count = local_cursor.execute(_each_query_sql)
            except pymysql.err.IntegrityError as integrity_err:
                _args = integrity_err.args
                if 'Duplicate entry' in _args[1]:
                    # 当出现 duplicate entry 时候，使用 Insert Ignore 代替（replace into 会出现 duplicate error，暂时不知道原因）
                    is_replace = False
                    _each_query_sql = _each_query_sql.replace('REPLACE INTO', 'INSERT IGNORE INTO')
                    replace_count = local_cursor.execute(_each_query_sql)
                else:
                    logger.exception(msg="[table_name: {}][error_sql: {}]".format(each_table, _each_query_sql),
                                     exc_info=integrity_err)
                    continue
            except Exception as e:
                logger.exception(msg="[table_name: {}][error_sql: {}]".format(each_table, _each_query_sql), exc_info=e)
                continue
            logger.debug(
                "[insert data][to: {}][from: {}][update_time: {}][final_update_time: {}][limit: {}][line_count: {}]["
                "{}: {}][takes: {}]".format(
                    to_table_name if 'poi_merge' not in _each_query_sql else 'poi_merge.attr' if 'poi_merge.attr' in _each_query_sql else 'poi_merge.rest',
                    each_table,
                    u_time,
                    final_update_time,
                    limit,
                    line_count,
                    'replace_count' if is_replace else 'insert_ignore_count',
                    replace_count,
                    time.time() - start
                ))
        local_conn.commit()
        local_cursor.close()

        update_seek_table(each_table, final_update_time)
    local_conn.close()


def main():
    create_table()
    load_data(limit=5000)


if __name__ == '__main__':
    main()
