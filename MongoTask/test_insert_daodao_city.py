#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午4:18
# @Author  : Hou Rong
# @Site    : 
# @File    : test_insert_qyer_city.py
# @Software: PyCharm
import urllib.parse
from data_source import MysqlSource
from logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config

logger = get_logger("insert_mongo_task")


def get_tasks():
    query_sql = '''SELECT *
FROM ota_location
WHERE source = 'daodao' AND city_id in ('11444','60177','12344','60178','10436','60179','60180','30118','30140','50053','60181','10648','11424','60182','60183','50117','20096');'''

    for _l in MysqlSource(db_config=source_info_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=True):
        yield _l


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.poi_list_task', queue='poi_list', routine_key='poi_list',
                    task_name='city_attr_daodao_20171214a', source='Daodao', _type='PoiList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for line in get_tasks():
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
                'poi_type': 'attr'
            }

            it.insert_task(args)
