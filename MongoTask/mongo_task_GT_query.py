#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @author: feng

# @date: 2018-02-01
from data_source import MysqlSource
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from service_platform_conn_pool import source_info_config
import pymongo


def get_tasks():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['CtripGTCity_Mioji']
    tasks = []
    for co in collections.find({}):
        tasks.append(co)
    return tasks


if __name__ == '__main__':
    #
    # ctrip or tuniu
    #
    args = []
    for task in get_tasks():
        if task['len_id'] != 1:
            continue
        args.append({
            "dept_info": {
                "id": "1",
                "name": "北京",
                "name_en": "Beijing"
            },
            "dest_info": {
                "id": task['city_id'],
                "name": task['name'],
                "name_en": 'tour'
            },
            "vacation_type": "grouptravel",

            'source':'ctrip'
        })
    with InsertTask(worker='proj.total_tasks.GT_list_task', queue='poi_list', routine_key='poi_list',
                    task_name='city_total_GT_20180306a', source='GT', _type='GTList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for line in args:

            it.insert_task(line)
