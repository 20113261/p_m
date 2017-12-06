#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/4 下午1:13
# @Author  : Hou Rong
# @Site    : 
# @File    : new_qyer_detail_task.py
# @Software: PyCharm
import pandas
from MongoTask.MongoTaskInsert import InsertTask

# f = open('/Users/hourong/Downloads/google_drive_url.txt')
# f = open('/search/hourong/task/target_url_1128')
table = pandas.read_csv("/tmp/qyer_result.csv")
_count = 0
with InsertTask(worker='proj.total_tasks.qyer_detail_task', queue='poi_detail', routine_key='poi_detail',
                task_name='detail_total_qyer_20171201a', source='Qyer', _type='QyerDetail',
                priority=11) as it:
    for i in range(len(table)):
        line = table.iloc[i]
        map_info = line['map_info']
        if map_info == '0.000000,0.000000':
            continue
        args = {
            'target_url': line['url'],
            'part': 'detail_total_qyer_20171201a',
            'city_id': 'NULL'
        }
        it.insert_task(args=args)
        _count += 1
    print(_count)

    # for line in f:
    #     g_url, flag, table_name = line.strip().split('###')
    #     it.insert_task({
    #         'url': g_url,
    #         'flag': flag,
    #         'table_name': table_name
    #     })
