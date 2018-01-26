#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/26 下午5:17
# @Author  : Hou Rong
# @Site    : 
# @File    : inner_city_task.py
# @Software: PyCharm
# !/usr/bin/python
# coding=utf-8
from MongoTask.MongoTaskInsert import InsertTask

if __name__ == '__main__':
    cids = ['40050', '40051', '40052', '40053', '51516', '51517', '51518', '51519', '51520', '51521', '51522']

    # todo 读取生成的 urls 中的文件，生成相应的任务，防止巴厘岛这种 6000 万任务的情况，需要对最终的结果进行特殊的甄别
    with InsertTask(worker='proj.total_tasks.google_drive_task', queue='file_downloader', routine_key='file_downloader',
                    task_name='google_drive_task_20180126', source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        pass
        it.insert_task({
            'url': 'Hello World',
        })
