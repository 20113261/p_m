#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/22 上午11:04
# @Author  : Hou Rong
# @Site    : 
# @File    : task_progress_report.py
# @Software: PyCharm
import redis
import datetime
import dataset
import json
import pymysql
from collections import defaultdict
from logger import get_logger

logger = get_logger("task_progress_redis")


def main():
    ori_ip = '10.10.228.253'
    ori_user = 'mioji_admin'
    ori_password = 'mioji1109'
    ori_db_name = 'ServicePlatform'

    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    product_table = db['serviceplatform_product_summary']
    product_error_table = db['serviceplatform_product_error_summary']

    r = redis.Redis(host='10.10.180.145', db=9)
    dt = datetime.datetime.now()
    product_count = defaultdict(int)
    product_error_count = defaultdict(int)

    for key in r.keys():
        key_list = key.decode().split('|_|')
        count = r.get(key)

        if len(key_list) == 5:
            task_tag, crawl_type, task_source, task_type, report_key = key_list
            product_count[(task_tag, crawl_type, task_source, task_type, report_key)] = int(count)
        elif len(key_list) == 6:
            task_tag, crawl_type, task_source, task_type, report_key, task_error_code = key_list
            product_error_count[(task_tag, crawl_type, task_source, task_type, report_key, task_error_code)] = int(
                count)
        else:
            logger.debug("[unknown key][key: {}]".format(key))
            continue

    # 列表页城市统计相关内容
    local_conn = pymysql.connect(host=ori_ip, user=ori_user, charset='utf8', passwd=ori_password, db=ori_db_name)

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'ServicePlatform' AND TABLE_NAME LIKE 'list_%';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    for table_name in table_list:
        key_list = table_name.split('_')

        if len(key_list) == 4:
            # 总量计数
            local_cursor = local_conn.cursor()
            local_cursor.execute('''SELECT count(DISTINCT city_id)
                        FROM {0};'''.format(table_name))
            _count = local_cursor.fetchone()[0]
            local_cursor.close()

            task_type, crawl_type, task_source, task_tag = key_list
            product_count[(task_tag, crawl_type.title(), task_source.title(), task_type.title(), "CityDone")] = _count

    # 更新产品统计
    for key, value in product_count.items():
        task_tag, crawl_type, task_source, task_type, report_key = key
        data = {
            'tag': task_tag,
            'source': task_source,
            'crawl_type': crawl_type,
            'type': task_type,
            'report_key': report_key,
            'num': value,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_table.upsert(data, keys=['tag', 'source', 'crawl_type', 'type', 'report_key', 'date'],
                                 ensure=None)
            logger.debug("[final data][{}]".format(json.dumps(data, indent=4, sort_keys=True)))
        except Exception as exc:
            logger.exception(msg="[update task progress table exception]", exc_info=exc)

        print(json.dumps(data, indent=4, sort_keys=True))

    # 更新产品错误统计
    for key, value in product_error_count.items():
        task_tag, crawl_type, task_source, task_type, report_key, product_error_count = key
        data = {
            'tag': task_tag,
            'source': task_source,
            'crawl_type': crawl_type,
            'type': task_type,
            'error_code': product_error_count,
            'num': value,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_error_table.upsert(data, keys=['tag', 'crawl_type', 'source', 'type', 'error_code', 'date'],
                                       ensure=None)
            logger.debug("[final data][{}]".format(json.dumps(data, indent=4, sort_keys=True)))
        except Exception as exc:
            logger.exception(msg="[update task progress table exception]", exc_info=exc)


if __name__ == '__main__':
    main()
