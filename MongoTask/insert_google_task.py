#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/21 下午9:31
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_google_task.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask

# f = open('/Users/hourong/Downloads/google_drive_url.txt')
f = open('/search/hourong/task/target_url_1128')
with InsertTask(worker='proj.total_tasks.crawl_json', queue='file_downloader', routine_key='file_downloader',
                task_name='google_drive_task_20171129', source='Google', _type='GoogleDriveTask',
                priority=11) as it:
    for line in f:
        g_url, flag, table_name = line.strip().split('###')
        it.insert_task({
            'url': g_url,
            'flag': flag,
            'table_name': table_name
        })
