#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/12 下午8:47
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_move_img_task.py
# @Software: PyCharm
from logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from UpdateTable.ImgErrorMd5Search import used_file_name

logger = get_logger("insert_mongo_task")


def get_tasks():
    s = used_file_name()
    yield from s


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.ks_move_task', queue='file_downloader',
                    routine_key='file_downloader', task_name='ks_move_task_20171212a',
                    source='KinSoft', _type='MoveImg',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for f_name in get_tasks():
            args = {
                'from_bucket': 'mioji-attr',
                'to_bucket': 'mioji-shop',
                'file_name': f_name
            }
            it.insert_task(args)
