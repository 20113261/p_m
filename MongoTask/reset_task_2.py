#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/8 下午5:39
# @Author  : Hou Rong
# @Site    : 
# @File    : reset_task_2.py
# @Software: PyCharm
import pymongo
from service_platform_conn_pool import fetchall, spider_data_base_data_pool
from my_logger import get_logger
from data_source import MysqlSource

logger = get_logger("reset_img_merge_task")
client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task_Queue_merge_task_TaskName_merge_hotel_image_20180110_20']

base_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


def reset_task(uid_list):
    res = collections.update({
        'args.uid': {
            '$in': uid_list
        }
    }, {
        '$set': {
            'finished': 0,
            'running': 0,
            'used_times': 0
        }
    }, multi=True)
    logger.info("[reset_task][res: {}]".format(res))


def get_task():
    sql = '''SELECT uid FROM hotel_unid WHERE source='accor';'''
    u_l = []
    _count = 0
    for line in MysqlSource(base_data_config, table_or_query=sql,
                            size=10000, is_table=False):
        _count += 1
        u_l.append(line[0])
        if len(u_l) % 5000 == 0:
            reset_task(u_l)
            logger.info("[total: {}]".format(_count))
    if u_l:
        reset_task(u_l)


if __name__ == '__main__':
    get_task()
