#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午6:52
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_ihg_city_sugg.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from my_logger import get_logger
from service_platform_conn_pool import verify_info_new_pool, fetchall

logger = get_logger("insert_mongo_task")


def generate_key(s):
    if len(s) > 3:
        yield s[:3].strip()
    elif len(s) > 6:
        yield s[:6].strip()
    yield s.strip()


def get_tasks():
    query_sql = '''SELECT DISTINCT name_en
FROM city;'''
    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_city
FROM ota_location
WHERE s_city REGEXP '[a-z]' = 1 AND s_region = 'NULL';'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT s_region
FROM ota_location
WHERE s_region REGEXP '[a-z]' = 1 AND s_region != 'NULL';'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])


if __name__ == '__main__':
    # _count = 0
    # for c_name in get_tasks():
    #     _count += 1
    #     if _count % 1000 == 0:
    #         print(c_name)
    with InsertTask(worker='proj.total_tasks.ihg_city_suggest', queue='supplement_field',
                    routine_key='supplement_field',
                    task_name='igh_search_20171219a', source='Ihg', _type='CityInfo',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for c_name in get_tasks():
            args = {
                'keyword': c_name
            }
            it.insert_task(args)
