#!/usr/bin/env python
# -*- coding:utf-8 -*-

from MongoTask.MongoTaskInsert import InsertTask, TaskType
from my_logger import get_logger
from service_platform_conn_pool import verify_info_new_pool, fetchall
from datetime import datetime
import pymongo

MONGODB_CONFIG = {
    'host': '10.10.213.148'
}

def get_english_keyword():
    def generate_key(s):
        if len(s) > 3:
            yield s[:3].strip()
        elif len(s) > 6:
            yield s[:6].strip()
        yield s.strip()

    query_sql = '''SELECT DISTINCT name_en
    FROM city;'''
    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_city
    FROM ota_location
    WHERE s_city REGEXP '[a-z]' = 1 AND s_region = 'NULL';'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT s_region
    FROM ota_location
    WHERE s_region REGEXP '[a-z]' = 1 AND s_region != 'NULL';'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

def get_china_keyword():
    def generate_key(s):
        if len(s) > 0:
            yield s[0]
        if len(s) > 1:
            yield s[:2]
        if len(s) > 2:
            yield s[:3]
        yield s

    query_sql = '''SELECT DISTINCT name
       FROM city;'''
    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_city
FROM ota_location
WHERE  s_city REGEXP '[\\u0391-\\uFFE5]' = 0;'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

    query_sql = '''SELECT DISTINCT s_region
FROM ota_location
WHERE s_region REGEXP '[\\u0391-\\uFFE5]' = 0;'''

    for line in fetchall(verify_info_new_pool, query_sql):
        yield from generate_key(line[0])

def create_mongodb_collect(source):
    client = pymongo.MongoClient(**MONGODB_CONFIG)
    local_time = str(datetime.now())[:10].replace('-','')
    collection_name = '{0}_city_suggest_{1}'.format(source,local_time)
    db = client['CitySuggest']
    collection = db[collection_name]
    return collection.name


def crawl_suggest():
    source = 'bestwest'
    collection_name = create_mongodb_collect(source)
    task_name = "all_city_suggest_{0}"
    local_time = str(datetime.now())[:10].replace('-','')
    task_name = task_name.format(local_time)
    with InsertTask(worker='proj.total_tasks.all_city_suggest', queue='supplement_field',
                    routine_key='supplement_field',
                    task_name=task_name, source='bestwest', _type='CityInfo',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for c_name in get_english_keyword():
            args = {
                'keyword': c_name,
                'spider_tag': 'bestwestSuggest',
                'collection_name': collection_name,
                'source': 'bestwest',
                'key':'AIzaSyAkKayctgUFqqA9Mp66CxzRDUrOX4zDQFc'
            }
            it.insert_task(args)
if __name__ == "__main__":
    crawl_suggest()