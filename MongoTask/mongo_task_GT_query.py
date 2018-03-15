#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @author: feng

# @date: 2018-02-01
from data_source import MysqlSource
from MongoTask.MongoTaskInsert import InsertTask, TaskType
import pymongo


def get_ctrip_dept():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['ctrip_vacation_suggestion']
    task = []
    for co in collections.find({}):
        task.append(co)
    return  task
def get_ctrip_tasks():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['CtripGTCity_Mioji']
    tasks = []
    for co in collections.find({}):
        tasks.append(co)
    return tasks

def get_tuniu_tasks():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['TuniuGTCity']
    tasks = []
    for co in collections.find({}):
        tasks.append(co)
    return tasks
def get_tuniu_dept():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['tuniu_vacation_suggestion']
    for co in collections.find({}):
        if co['cities']==None:
            continue
        for c in co['cities']:
            tasks = {'name':c['cityName'],'name_en':c['cityLetter'],'pid':c['cityCode']}
            yield tasks

def insert_ctrip_task():
    args = []
    for co in get_ctrip_dept():
        for c in get_ctrip_tasks():
            if c['len_id'] != 1:
                continue
            args.append({
                "dept_info": {
                    "id": str(co['City']),
                    "name": co['Name'],
                    "name_en": co['Ename']
                },
                "dest_info": {
                    "id": str(c['id']),
                    "name": c['name'],
                    "name_en": 'tour'
                },
                "vacation_type": "grouptravel",

                'source': 'ctrip'
            })
    with InsertTask(worker='proj.total_tasks.GT_list_task', queue='grouptravel', routine_key='grouptravel',
                    task_name='city_total_GT_20180312a', source='GT', _type='GTList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for line in args:

            it.insert_task(line)

def insert_tuniu_task():
    with InsertTask(worker='proj.total_tasks.GT_list_task', queue='grouptravel', routine_key='grouptravel',
                    task_name='city_total_GT_20180314a', source='GT', _type='GTList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for co in get_tuniu_dept():
            for c in get_tuniu_tasks():
                args={
                    "dept_info": {
                        "id": co['pid'],
                        "name": co['name'],
                        "name_en": co['name_en']
                    },
                    "dest_info": {
                        "id": str(c['id']),
                        "name": c['name'],
                        "name_en": 'tour'
                    },
                    "vacation_type": "grouptravel",

                    'source': 'tuniu'
                }
                it.insert_task(args)



if __name__ == '__main__':
    insert_tuniu_task()
    #
    # ctrip or tuniu
    #

    # for task in get_ctrip_tasks():
    #     if task['len_id'] != 1:
    #         continue
    #     args.append({
    #         "dept_info": {
    #             "id": "1",
    #             "name": "北京",
    #             "name_en": "Beijing"
    #         },
    #         "dest_info": {
    #             "id": str(task['id']),
    #             "name": task['name'],
    #             "name_en": 'tour'
    #         },
    #         "vacation_type": "grouptravel",
    #
    #         'source':'ctrip'
    #     })
#------
    # for task in get_tuniu_tasks():
    #
    #     args.append({
    #         "dept_info": {
    #             "id": "1",
    #             "name": "北京",
    #             "name_en": "Beijing"
    #         },
    #         "dest_info": {
    #             "id": str(task['id']),
    #             "name": task['name'],
    #             "name_en": 'tour'
    #         },
    #         "vacation_type": "grouptravel",
    #
    #         'source':'tuniu'
    #     })
# ---- test
#     args.append({
#         "dept_info": {
#             "id": "1",
#             "name": "北京",
#             "name_en": "Beijing"
#         },
#         "dest_info": {
#             "id": "569",
#             "name": "塞班岛",
#             "name_en": "tour"
#         },
#         "vacation_type": "grouptravel",
#         "source": 'ctrip'
#     })
