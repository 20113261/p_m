#!/usr/bin/env python
# -*- coding:utf-8 -*-


from MongoTask.MongoTaskInsert import InsertTask
from datetime import datetime
import csv
import pandas
import json
from city.find_hotel_opi_city import add_city_suggest

def get_task_name(param):
    task_name = "all_suggest_{0}"
    local_time = str(datetime.now())[:10].replace('-','')
    local_time = ''.join([local_time,param])
    task_name = task_name.format(local_time)
    return task_name


def create_task(city_path,path,database_name,param):
    task_name = get_task_name(param)

    with InsertTask(worker='proj.total_tasks.allhotel_city_suggest', queue='supplement_field', routine_key='supplement_field',
                    task_name=task_name, source='sources', _type='SourceSuggest',
                    priority=11) as it:
        citys = add_city_suggest(city_path)
        for source,citys in citys.items():
            for city in citys:
                args = {
                    'source': source,
                    'keyword': city[0],
                    'country_id': str(city[1]),
                    'map_info': city[2],
                    'city_id': None,
                    'database_name': database_name
                }
                it.insert_task(args)
        collection_name = it.generate_collection_name()
        return collection_name,task_name

if __name__ == "__main__":
    create_task()