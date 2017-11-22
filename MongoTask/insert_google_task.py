#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/21 下午9:31
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_google_task.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask

f = open('/Users/hourong/Downloads/google_drive_url.txt')
for line in f:
    with InsertTask(worker='proj.total_tasks.crawl_json', queue='file_downloader', routine_key='file_downloader',
                    task_name='google_drive_task_20171121', source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        it.insert_task({
            'url': line.strip(),
            'flag': '1121',
            'table_name': 'new_crawled_html_455'
        })
