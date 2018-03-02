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

def get_city_id(path):
    city_id = {}
    with open(path+'city_id.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            city_id[int(row['city_id_number'])] = city_id[row['city_id']]
    return city_id
def create_task(city_path,path,database_name):
    task_name = get_task_name()
    city_map_id = get_city_id(path)
    with InsertTask(worker='proj.total_tasks.allhotel_city_suggest', queue='poi_detail', routine_key='poi_detail',
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
                    'city_id': city_map_id[int(city[3])],
                    'database_name': database_name
                }
                it.insert_task(args)
        collection_name = it.generate_collection_name()
        return collection_name,task_name

if __name__ == "__main__":
    create_task()