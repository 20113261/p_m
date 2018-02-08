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
    collections = client['SuggestName']['CtripPoiSDK_mioji']
    #tasks =set()
    #for data in collections.find():
    #    try:
    #        for d in data['suggest']['List']:
    #            ur = d['Url'].split('/')[2]
    #            tasks.add(ur.split('.')[0])
    #            if len(tasks) == 10:
    #                break
    #    except:
    #        pass
    #    if len(tasks)==10:
    #        break
    #print(len(tasks))
    tasks = []
    for co in collections.find({}):
        tasks.append(co['task'])
        if len(tasks) == 10:
            break

    return tasks


if __name__ == '__main__':
    with InsertTask(worker='proj.total_tasks.ctrip_poi_list_task', queue='poi_list', routine_key='poi_list',
                    task_name='poi_total_ctripPoi_20180208a', source='CtripPoi', _type='CtripList',
                    priority=3, task_type=TaskType.LIST_TASK) as it:
        for line in get_tasks():
            args = {
                "city_id": "000",
                "country_id": "000",
                "source": "ctripPoi",
                'city_url': line
            }
            it.insert_task(args)
