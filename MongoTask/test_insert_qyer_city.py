#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午4:18
# @Author  : Hou Rong
# @Site    : 
# @File    : test_insert_qyer_city.py
# @Software: PyCharm
from data_source import MysqlSource
from logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config

logger = get_logger("insert_mongo_task")


def get_tasks():
    query_sql = '''SELECT *
FROM ota_location
WHERE source = 'qyer';'''

    for _l in MysqlSource(db_config=source_info_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.qyer_list_task', queue='poi_list', routine_key='poi_list',
                    task_name='city_total_qyer_20171120a', source='Qyer', _type='QyerList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for line in get_tasks():
            args = {
                'city_id': line['city_id'],
                'country_id': line['country_id'],
                'source': line['source'],
                'city_url': line['suggest']
            }
            it.insert_task(args)
