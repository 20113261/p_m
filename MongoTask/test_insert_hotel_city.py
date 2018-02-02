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

logger = get_logger("insert_mongo_task")


def get_tasks(source):
    # query_sql = '''SELECT *
    # FROM ota_location
    # WHERE source = '{}' AND city_id in ('11444','60177','12344','60178','10436','60179','60180','30118','30140','50053','60181','10648','11424','60182','60183','50117','20096');'''.format(
    #     source)

    query_sql = '''SELECT *
    FROM ota_location
    WHERE source = '{}';'''.format(
        source)

    #     query_sql = '''SELECT *
    # FROM ota_location
    # WHERE source = '{}' AND city_id in ('20096');'''.format(
    #         source)

    for _l in MysqlSource(db_config=source_info_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l


if __name__ == '__main__':
    # source_list = ['booking', 'agoda', 'ctrip', 'hotels', 'expedia', 'elong']
    # source_list = ['expedia']
    # source_list = ['elong']
    # source_list = ['agoda', 'hotels', 'expedia', 'elong']
    # source_list = ['ctrip']
    # source_list = ['expedia']
    # source_list = ['hotels']
    source_list = ['ihg']

    for source in source_list:
        task_name = 'city_hotel_{}_20171222a'.format(source)
        with InsertTask(worker='proj.total_tasks.hotel_list_task', queue='hotel_list', routine_key='hotel_list',
                        task_name=task_name, source=source.title(), _type='HotelList',
                        priority=3, task_type=TaskType.CITY_TASK) as it:
            for line in get_tasks(source=source):
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
                }

                it.insert_task(args)
