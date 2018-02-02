#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午6:52
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_qyer_city_sugg.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from my_logger import get_logger
from MongoTask.veriflight_code import veriflight_code
from string import ascii_lowercase
from itertools import product

logger = get_logger("insert_mongo_task")


def get_tasks():
    # yield from veriflight_code
    for chars in product(ascii_lowercase, repeat=3):
        yield ''.join(chars).upper()


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.veriflight_task', queue='supplement_field',
                    routine_key='supplement_field',
                    task_name='veriflight_20180111a', source='Veriflight', _type='IataCode',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for i_code in get_tasks():
            args = {
                'iata_code': '{}'.format(i_code)
            }
            it.insert_task(args)
