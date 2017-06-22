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


def get_task_hotel_raw():
    conn = pymysql.connect(user='hourong', passwd='hourong', db='Task', charset="utf8")
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  args,
  task_name
FROM TaskForBookingStar;
''')
    for parent_task_id, args, task_name in cursor.fetchall():
        j_data = json.loads(args)
        yield parent_task_id, task_name, j_data['source'], j_data['other_info']['source_id'], j_data['other_info'][
            'city_id'], j_data['hotel_url']


if __name__ == '__main__':
    task_name = 'hotel_static_base_data_170622'

    with InsertTask(worker='hotel_static_base_data', task_name=task_name) as it:
        for parent_task_id, task_name, source, source_id, city_id, hotel_url in get_task_hotel_raw():
            args = {
                'parent_task_id': parent_task_id,
                'task_name': task_name,
                'source': source,
                'source_id': source_id,
                'city_id': city_id,
                'hotel_url': hotel_url
            }
            it.insert_task(args=args)
