#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/14 上午10:46
# @Author  : Hou Rong
# @Site    : 
# @File    : detail_list_crawl_diff.py
# @Software: PyCharm
import pymongo
from logger import get_logger
from service_platform_conn_pool import service_platform_pool, fetchall

logger = get_logger('check_list_crawl_diff')

RespClient = pymongo.MongoClient(host='10.10.213.148')
RespDB = RespClient['data_result']

check_collection = [
    # ('Qyer20171214a', 'Task_Queue_poi_list_TaskName_list_total_qyer_20171209a', 'detail_total_qyer_20171209a'),
    ('qyer', 'Task_Queue_poi_list_TaskName_list_total_qyer_20171214a', 'detail_total_qyer_20171214a')
]


def task_resp_url(collection_name, task_collection_name):
    __set = set()
    resp_collections = RespDB[collection_name]
    _count = 0
    for line in resp_collections.find({'collections': task_collection_name}):
        # result loop
        for each in line['result']:
            _count += 1
            if _count % 10000 == 0:
                logger.info("[mongo url prepare: {}]".format(_count))
            __set.add(each[1])
    return __set


def mysql_resp_url(mysql_table_name):
    __set = set()
    sql = '''SELECT url
FROM {};'''.format(mysql_table_name)
    _count = 0
    for line in fetchall(service_platform_pool, sql):
        _count += 1
        if _count % 10000 == 0:
            logger.info("[mysql url prepare: {}]".format(_count))
        __set.add(line[0])
    return __set


if __name__ == '__main__':
    for c_name, t_c_name, m_t_name in check_collection:
        s_set = task_resp_url(c_name, t_c_name)
        d_set = mysql_resp_url(m_t_name)

        diff_1 = s_set - d_set

        diff_2 = d_set - s_set

        logger.info(


            '[tables: {}][diff_1][count: {}][res: {}]'.format((c_name, t_c_name, m_t_name), len(diff_1), diff_1))
        logger.info(
            '[tables: {}][diff_2][count: {}][res: {}]'.format((c_name, t_c_name, m_t_name), len(diff_2), diff_2))
