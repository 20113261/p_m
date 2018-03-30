#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/18 上午10:30
# @Author  : Hou Rong
# @Site    : 
# @File    : google_address.py
# @Software: PyCharm
import pymysql
import pymongo
from bson.objectid import ObjectId
import json
from MongoTask.MongoTaskInsert import InsertTask


# from service_platform_conn_pool import source_info_pool, fetchall
client = pymongo.MongoClient(host='10.10.213.148')
collections = client['base_data']['station_new']

def get_tasks():
    db = pymysql.connect(host='10.10.230.206', user='mioji_admin', passwd='mioji1109', db='source_info', charset='utf8')
    cur = db.cursor()
    try:
        sql = '''SELECT
      sid,
      s_city,
      s_country,
      city_id,
      country_id,
      others_info
    FROM ota_location_european_online;'''
        cur.execute(sql)
        yield from cur.fetchall()
    except Exception as e:
        print('err', e)
    cur.close()
    db.close()

def get_mongo_task():
    yield from collections.find({}, {'city_name': 1, 'country_code': 1, 'station_name': 1})


if __name__ == '__main__':
    # for g in get_tasks():
    #     print({
    #         'city_code': g[0],
    #         'city_name': g[1],
    #         'country_code': g[2],
    #         'city_id': g[3],
    #         'country_id': g[4],
    #         'inventory': json.loads(g[5])["Inventory"]
    #     })
    with InsertTask(worker='proj.total_tasks.european_trail_task',
                    queue='file_downloader',
                    routine_key='file_downloader',
                    task_name='european_trail_20180329a',
                    source='European',
                    _type='Trail',
                    priority=3) as it:
        # for g in get_tasks():
        #     it.insert_task(args={
        #         'city_code': g[0],
        #         'city_name': g[1],
        #         'country_code': g[2],
        #         'city_id': g[3],
        #         'country_id': g[4],
        #         'inventory': json.loads(g[5])["Inventory"]
        #     })
        for line in get_mongo_task():
            it.insert_task(args={
                'city_name': line['city_name'],
                'station_name': line['station_name'],
                'country_code': line['country_code'],
                'id': str(line['_id'])
            })
    # get_mongo_task()