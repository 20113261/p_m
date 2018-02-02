#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午4:18
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_qyer_city.py
# @Software: PyCharm
import json
from data_source import MysqlSource
from my_logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config
from datetime import datetime
logger = get_logger("insert_mongo_task")


def get_tasks(source,city_id=None):
    query_sql = '''SELECT *
    FROM ota_location
    WHERE source = '{0}' AND city_id in {1};'''.format(source,tuple(city_id))

    # query_sql = '''SELECT *
    # FROM ota_location
    # WHERE source = '{}';'''.format(
    #     source)

    #     query_sql = '''SELECT *
    # FROM ota_location
    # WHERE source = '{}' AND city_id in ('20096');'''.format(
    #         source)

    for _l in MysqlSource(db_config=source_info_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l

def hotel_city(city_id,param,sources):
    source_list = sources
    collections_name = []
    for source in source_list:
        time_lag = str(datetime.now())[:10].replace('-', '')
        task_name = 'hotel_{0}_{1}_{2}a'.format(source,param,time_lag)
        with InsertTask(worker='proj.total_tasks.hotel_list_task', queue='hotel_list', routine_key='hotel_list',
                        task_name=task_name, source=source.title(), _type='HotelList',
                        priority=3, task_type=TaskType.CITY_TASK) as it:
            for line in get_tasks(source=source,city_id=city_id):
                suggest = line['suggest']
                line['suggest_type'] = str(line['suggest_type'])
                if line['suggest_type'] == '2':
                    if line["source"] != "ctrip":
                        try:
                            tmp_sug = json.loads(suggest)
                        except Exception as exc:
                            tmp_sug = eval(suggest)
                            suggest = json.dumps(tmp_sug)
                    else:
                        pass

                args = {
                    'source': source,
                    'city_id': line['city_id'],
                    'country_id': line['country_id'],
                    'part': task_name.split('_')[-1],
                    'is_new_type': 1,
                    'suggest_type': line['suggest_type'],
                    'suggest': suggest,
                    'task_id': 'inner_{0}'.format(param)
                }

                it.insert_task(args)
            collections_name.append(it.generate_collection_name())
    return collections_name
if __name__ == '__main__':
    # source_list = ['booking', 'agoda', 'ctrip', 'hotels', 'expedia', 'elong']
    # source_list = ['expedia']
    # source_list = ['elong']
    # source_list = ['agoda', 'hotels', 'expedia', 'elong']
    # source_list = ['ctrip']
    # source_list = ['expedia']
    # source_list = ['hotels']
    pass
