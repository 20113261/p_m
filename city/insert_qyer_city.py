#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午4:18
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_qyer_city.py
# @Software: PyCharm
from data_source import MysqlSource
from my_logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config
from datetime import datetime
import copy

logger = get_logger("insert_mongo_task")


def get_tasks(city_id,config):
    query_sql = '''SELECT *
FROM ota_location
WHERE source = 'qyer' AND city_id in {};'''.format(tuple(city_id))

    for _l in MysqlSource(db_config=config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l

def qyer_city(city_id,param,config):
    task_name = "city_total_qyer_{}{}"
    time_lag = str(datetime.now())[:10].replace('-','')
    task_name = task_name.format(param,time_lag)
    with InsertTask(worker='proj.total_tasks.qyer_list_task', queue='poi_list', routine_key='poi_list',
                    task_name=task_name, source='Qyer', _type='QyerList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        temp_config = copy.deepcopy(config)
        temp_config['database'] = temp_config['db']
        del temp_config['db']
        del temp_config['charset']
        for line in get_tasks(city_id,temp_config):
            args = {
                'city_id': line['city_id'],
                'country_id': line['country_id'],
                'source': line['source'],
                'city_url': line['suggest'],
                'task_id': 'inner_{0}'.format(param)
            }
            it.insert_task(args)
        return it.generate_collection_name(),task_name
if __name__ == '__main__':
    pass
