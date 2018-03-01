#!/usr/bin/env python
# -*- coding:utf-8 -*-


from MongoTask.MongoTaskInsert import InsertTask
from datetime import datetime
import csv
import json
from city.find_hotel_opi_city import add_city_suggest
def get_task_name():
    task_name = "all_source_suggest_{0}"
    local_time = str(datetime.now())[:10].replace('-','')
    local_time = ''.join([local_time,'a'])
    task_name = task_name.format(local_time)
    return task_name
def create_task():
    with InsertTask(worker='proj.total_tasks.allhotel_city_suggest', queue='poi_detail', routine_key='poi_detail',
                    task_name=get_task_name(), source='sources', _type='SourceSuggest',
                    priority=11) as it:
        citys = add_city_suggest()
        for source,citys in citys:
            for city in citys:
                args = {
                    'source': source,
                    'keyword': city[0],
                    'country_id': city[1],
                    'map_info': city[2]
                }
                it.insert_task(args)


if __name__ == "__main__":
    create_task()