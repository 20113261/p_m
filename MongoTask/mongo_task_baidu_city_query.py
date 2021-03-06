#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午6:52
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_qyer_city_sugg.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from my_logger import get_logger
from service_platform_conn_pool import verify_info_new_pool, fetchall

logger = get_logger("insert_mongo_task")


def generate_key(s):
    if len(s) > 0:
        yield s[0]
    if len(s) > 1:
        yield s[:2]
    if len(s) > 2:
        yield s[:3]
    yield s


def get_tasks():
    query_sql = '''SELECT DISTINCT name
FROM city;'''
    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_city
FROM ota_location
WHERE  s_city REGEXP '[\\u0391-\\uFFE5]' = 0;'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_region
FROM ota_location
WHERE s_region REGEXP '[\\u0391-\\uFFE5]' = 0;'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.baidu_search_task', queue='supplement_field',
                    routine_key='supplement_field',
                    task_name='baidu_search_20171211a', source='Baidu', _type='CityInfo',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for c_name in get_tasks():
            args = {
                'keyword': 'site:place.qyer.com {}'.format(c_name)
            }
            it.insert_task(args)
