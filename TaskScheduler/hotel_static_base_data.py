#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/20 上午10:31
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_static_base_data.py
# @Software: PyCharm
import pymysql
import json
from TaskScheduler.TaskInsert import InsertTask


def get_task_hotel_raw(old_task_name):
    # todo by task
    #     conn = pymysql.connect(user='hourong', passwd='hourong', db='Task', charset="utf8")
    #     cursor = conn.cursor()
    #     cursor.execute('''SELECT
    #   id,
    #   args,
    #   task_name
    # FROM TaskForBookingStar;
    # ''')
    #     for parent_task_id, args, task_name in cursor.fetchall():
    #         j_data = json.loads(args)
    #         yield parent_task_id, task_name, j_data['source'], j_data['other_info']['source_id'], j_data['other_info'][
    #             'city_id'], j_data['hotel_url']
    # todo by mongo
    import pymongo

    client = pymongo.MongoClient(host='10.10.231.105', port=27017)
    collections = client['PageSaver'][old_task_name]

    # 获取最小，最大 id 以便遍历全部
    min_id = list(collections.find().sort('_id', 1).limit(1))[0]['_id']
    max_id = list(collections.find().sort('_id', -1).limit(1))[0]['_id']

    now_id = min_id

    # 前两千采用大于等于，因为需要包含第一项
    for line in collections.find({'_id': {'$gte': now_id}}).sort('_id').limit(2000):
        now_id = line['_id']
        yield line['task_id'], line['source'], line['source_id'], line['city_id'], line['url']
        if now_id == max_id:
            break

    while True:
        for line in collections.find({'_id': {'$gt': now_id}}).sort('_id').limit(2000):
            now_id = line['_id']
            yield line['task_id'], line['source'], line['source_id'], line['city_id'], line['url']
            if now_id == max_id:
                # 采用异常跳出两级循环
                raise Exception("End Of Iter")


if __name__ == '__main__':
    old_task_name = 'hotel_base_data_ctrip'
    task_name = 'hotel_static_base_data_170630_ctrip'

    with InsertTask(worker='hotel_static_base_data', task_name=task_name) as it:
        for parent_task_id, source, source_id, city_id, hotel_url in get_task_hotel_raw(old_task_name):
            args = {
                'parent_task_id': parent_task_id,
                'task_name': old_task_name,
                'source': source,
                'source_id': source_id,
                'city_id': city_id,
                'hotel_url': hotel_url
            }
            it.insert_task(args=args)
