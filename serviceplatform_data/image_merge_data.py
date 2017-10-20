#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 下午5:26
# @Author  : Hou Rong
# @Site    : 
# @File    : image_merge_data.py
# @Software: PyCharm
import time
import pymysql.err
from warnings import filterwarnings
from service_platform_conn_pool import service_platform_pool, base_data_final_pool
from logger import get_logger

# ignore pymysql warnings
filterwarnings('ignore', category=pymysql.err.Warning)

logger = get_logger("image_insert_final_data")

final_database = 'BaseDataFinal'

final_table = {
    "hotel": "hotel_images.sql",
    "attr": "poi_images.sql",
    "rest": "poi_images.sql",
    "total": "poi_images.sql"
}

image_type_dict = {
    "hotel": "hotel",
    "attr": "poi",
    "rest": "poi",
    "total": "poi"
}


def create_table(image_type):
    final_conn = base_data_final_pool.connection()
    final_cursor = final_conn.cursor()
    sql_name = final_table.get(image_type, None)
    if sql_name is None:
        raise TypeError("[Unknown View Type: {}]".format(image_type))
    final_sql = open(
        '/search/hourong/PycharmProjects/PoiCommonScript/serviceplatform_data/sql/{}'.format(sql_name)).read()
    table_name = "{}_images".format(image_type_dict[image_type])
    final_cursor.execute(final_sql % (table_name,))
    logger.debug('[create table][name: {}]'.format(table_name))
    final_cursor.close()
    final_conn.close()


def get_seek(table_name):
    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    sql = '''SELECT seek,locked
    FROM data_insert_id_seek WHERE task_name='{}';'''.format(table_name)
    local_cursor.execute(sql)
    _res = local_cursor.fetchone()
    local_cursor.close()
    local_conn.close()

    if _res is not None:
        _seek, _locked = _res
        return _seek, bool(_locked)
    else:
        return 0, False


def update_seek_table(table_name, seek):
    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''REPLACE INTO data_insert_id_seek VALUES (%s, %s, 0);''', (table_name, seek))
    logger.debug("[update seek table][table_name: {}][max_id: {}]".format(table_name, seek))
    local_conn.commit()
    local_cursor.close()
    local_conn.close()


def insert_data(limit=1000):
    logger.debug("start insert data")
    logger.debug("get all table name")

    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'ServicePlatform';''')

    # 强制要求按照 tag 的先后顺序排列
    table_list = list(
        sorted(
            filter(lambda x: x.startswith('images_'),
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-1]
        )
    )
    local_cursor.close()

    for each_table_final in table_list:
        start = time.time()
        seek = get_seek(each_table_final)

        try:
            _, task_type, task_source, task_tag = each_table_final.split('_')
        except Exception:
            logger.error('[Unknown Task Final: {}]'.format(each_table_final))
            continue

        create_table(task_type, task_tag)
        if task_type in ('hotel', 'attr', 'rest', 'total'):
            to_table_name = "{}_images_final_{}".format(image_type_dict[task_type], task_tag)
            local_cursor = local_conn.cursor()
            get_id_sql = '''SELECT id
    FROM {0}
    WHERE id > '{1}'
    ORDER BY id
    LIMIT {2};'''.format(each_table_final, seek, limit)

            line_count = local_cursor.execute(get_id_sql)
            if line_count == 0:
                continue
            # get final update time for inserting db next time
            final_seek = max(map(lambda x: x[0], local_cursor.fetchall()))
            local_cursor.close()

            # replace into final data
            local_cursor = local_conn.cursor()
            if task_type in ('attr', 'rest', 'total'):
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
    {2} where id > {3} ORDER BY id LIMIT {4};;'''.format(final_database, to_table_name, each_table_final, seek, limit)
            elif task_type == 'hotel':
                query_sql = '''REPLACE INTO {0}.{1} (source, source_id, pic_url, pic_md5, part, hotel_id, status, update_date, size, flag, file_md5)
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
    {2}
  WHERE id > {3}
  ORDER BY id
  LIMIT {4};'''.format(final_database, to_table_name, each_table_final, seek, limit)
            else:
                continue

            try:
                replace_count = local_cursor.execute(query_sql)
            except Exception as e:
                logger.exception(msg="[table_name: {}][error_sql: {}]".format(each_table_final, query_sql), exc_info=e)
                continue
            local_conn.commit()
            local_cursor.close()
        else:
            raise TypeError("Unknown Type: {}".format(task_type))

        update_seek_table(each_table_final, final_seek)
        logger.debug(
            "[insert data][to: {}][from: {}][seek: {}][final_seek: {}][limit: {}][line_count: {}]["
            "replace_count: {}][takes: {}]".format(
                to_table_name,
                each_table_final,
                seek,
                final_seek,
                limit,
                line_count,
                replace_count,
                time.time() - start
            ))
    local_conn.close()


if __name__ == '__main__':
    # insert_data(limit=5000)

    # test get seek
    get_seek("test")

    # test update seek
    update_seek_table("test", 3)

    # create table test
    create_table("attr")
