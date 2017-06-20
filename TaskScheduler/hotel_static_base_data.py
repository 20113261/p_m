#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/20 上午10:31
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_static_base_data.py
# @Software: PyCharm
import pymysql
from TaskScheduler.TaskInsert import InsertTask


def get_task_hotel_raw():
    conn = pymysql.connect(user='hourong', passwd='hourong', db='Task', charset="utf8")
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  task_name
FROM TaskForBookingStar WHERE finished=1;
''')
    for line in cursor.fetchall():
        yield line


if __name__ == '__main__':
    task_name = 'hotel_static_base_data_170620'
    it = InsertTask(worker='hotel_static_base_data', task_name=task_name)

    for parent_task_id, task_name in get_task_hotel_raw():
        args = {'parent_task_id': parent_task_id, 'task_name': task_name}
        it.insert_task(args=args)
