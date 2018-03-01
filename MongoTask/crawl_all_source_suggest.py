#!/usr/bin/env python
# -*- coding:utf-8 -*-


from MongoTask.MongoTaskInsert import InsertTask
from datetime import datetime
import csv
import json
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
        with open('deletion_city_suggest.csv','r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                source = row['source']
                citys = row['city_name']
                citys = json.loads(citys)
                for city in citys:
                    args = {
                        'source': source,
                        'keyword': city,
                    }
                    it.insert_task(args)


if __name__ == "__main__":
    create_task()