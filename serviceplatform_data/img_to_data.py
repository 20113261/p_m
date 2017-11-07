#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pymysql
import re
import json
from logger import get_logger
from data_source import MysqlSource
from toolbox.Common import is_legal
from service_platform_conn_pool import base_data_final_pool
from Common.Utils import retry
from toolbox.Hash import encode

logger = get_logger("img_to_data")

offset = 0

config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'port': 3306,
    'charset': 'utf8',
    'db': 'ServicePlatform',
}

to_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'port': 3306,
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}

to_data_conn = pymysql.connect(**to_data_config)


def get_table_name():
    conn = pymysql.connect(**config)
    cursor = conn.cursor(pymysql.cursors.SSDictCursor)
    get_name_sql = "select table_name from information_schema.tables where table_schema='ServicePlatform' and table_type='base table'"
    try:
        cursor.execute(get_name_sql)
        table_names = cursor.fetchall()
        temp = {}
        for table_name in table_names:
            temp_result = re.search(u'detail_hotel_[a-zA-Z0-9_]+', table_name[0])
            if temp_result:
                source, date = temp_result.group().split('_')[2:]
                temp['_'.join([source, date])] = date
            else:
                continue
        logger.debug('成功获取到的表名：', temp)
        return temp
    except Exception as e:
        logger.debug('在查询表名时出现错误：', e.message)
        conn.rollback()


@retry(times=3)
def insert_db(table_name, data):
    global offset
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    insert_into = "REPLACE INTO first_images (`source`, `source_id`, `first_img`) VALUES (%s, %s, %s)"
    res = cursor.executemany(insert_into, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.debug(
        "[insert data][table_name: {}][offset: {}][total: {}][insert: {}]".format(table_name, offset, len(data), res))


def to_data(table_name):
    global offset
    select_sql = '''SELECT
  source,
  source_id,
  others_info
FROM detail_hotel_{0}'''.format(table_name)
    try:
        _data = []
        for result in MysqlSource(db_config=config, table_or_query=select_sql,
                                  size=10000, is_table=False,
                                  is_dict_cursor=True):
            offset += 1
            others_info = result['others_info']
            if not others_info:
                continue
            others_info = json.loads(others_info)
            if 'first_img' not in others_info:
                continue
            first_img_url = others_info['first_img']

            if not is_legal(first_img_url):
                continue
            md5_str = encode(first_img_url)
            source = result['source']
            source_id = result['source_id']
            _data.append((source, source_id, md5_str))
            if len(_data) % 1000 == 0:
                insert_db(table_name, _data)
                _data = []
        insert_db(table_name, _data)
    except Exception as exc:
        logger.exception(msg="[入库出现异常]", exc_info=exc)


sources = ['ctrip', 'elong', 'expedia', 'agoda', 'booking', 'hotels']


def data_sort():
    global offset
    for date in ['20170929a', '20171010a']:
        for source in sources:
            offset = 0
            logger.debug('开始正在进行【{0}】表查询'.format('_'.join([source, date])))
            table_name = '_'.join([source, date])
            to_data(table_name)


if __name__ == "__main__":
    data_sort()
