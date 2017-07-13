#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/11 下午5:30
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_list_task.py
# @Software: PyCharm
import Common.DateRange
import dataset
from Common.DateRange import dates_tasks
from TaskScheduler.TaskInsert import InsertTask

Common.DateRange.DATE_FORMAT = '%Y%m%d'
db = dataset.connect('mysql+pymysql://reader:mioji1109@10.19.118.147/source_info?charset=utf8')

if __name__ == '__main__':
    with InsertTask(worker='hotel_list', task_name='ctrip_hotel_list_0711') as it:
        for line in db.query('''SELECT city_id
FROM hotel_suggestions_city
WHERE source = 'ctrip' AND select_index != -1 AND annotation != -1;'''):

            city_id = line['city_id']

            for day in dates_tasks(90, day_step=10, ignore_days=20):
                args = {'source': 'ctrip', 'city_id': city_id, 'check_in': day,
                        'part': '20170711'}
                it.insert_task(args)
