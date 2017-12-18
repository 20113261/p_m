#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/18 上午10:30
# @Author  : Hou Rong
# @Site    : 
# @File    : google_address.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask
from service_platform_conn_pool import source_info_pool, fetchall


def get_tasks():
    sql = '''SELECT concat('https://maps.googleapis.com/maps/api/geocode/json?address=', s_country, ',', s_city)
FROM ota_location_for_european_trail;'''
    for line in fetchall(source_info_pool, sql=sql):
        yield line[0]


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.crawl_json', queue='file_downloader', routine_key='file_downloader',
                    task_name='google_drive_task_20171215', source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        for target_url in get_tasks():
            it.insert_task({
                'url': target_url,
                'flag': it.task_name,
                'table_name': 'new_crawled_html_605'
            })
