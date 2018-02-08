#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午4:18
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_qyer_city.py
# @Software: PyCharm

import urllib

import urllib.parse

from data_source import MysqlSource
from my_logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config
from datetime import datetime
import copy
logger = get_logger("insert_mongo_task")


def get_tasks(city_id=None,config=None):
    query_sql = '''SELECT *
FROM ota_location
WHERE source = 'daodao' AND city_id in {0};'''.format(tuple(city_id))

    for _l in MysqlSource(db_config=config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l

def daodao_city(city_id, param, config):
    time_lag = str(datetime.now())[:10].replace('-', '')
    task_name = 'city_attr_daodao_{}{}'.format(time_lag, param)
    with InsertTask(worker='proj.total_tasks.poi_list_task', queue='poi_list', routine_key='poi_list',
                    task_name=task_name, source='Daodao', _type='PoiList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        temp_config = copy.deepcopy(config)
        temp_config['database'] = temp_config['db']
        del temp_config['db']
        del temp_config['charset']
        for line in get_tasks(city_id=city_id,config=temp_config):
            # args = {
            #     'city_id': line['city_id'],
            #     'country_id': line['country_id'],
            #     'source': line['source'],
            #     'city_url': line['suggest']
            # }

            args = {
                'source': 'daodao',
                'url': urllib.parse.urlparse(line['suggest']).path,
                'city_id': line['city_id'],
                'country_id': line['country_id'],
                'poi_type': 'attr',
                'task_id': 'inner_{0}'.format(param)
            }


            it.insert_task(args)

        return it.generate_collection_name(),task_name

if __name__ == '__main__':
    pass
