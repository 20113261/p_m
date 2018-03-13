#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @author: feng
# @date: 2018-02-01


from MongoTask.MongoTaskInsert import InsertTask, TaskType
import pymongo


def get_tasks():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['CtripPoiSDK_Mioji']
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
        tasks.append(co)

    return tasks


if __name__ == '__main__':

    with InsertTask(worker='proj.total_tasks.PoiSource_list_task', queue='poi_list', routine_key='poi_list',
                    task_name='city_total_ctripPoi_20180312a', source='PoiS', _type='PoiSList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for line in get_tasks():
            args = {
                "city_id": line['city_id'],
                "country_id": "",
                "source": "ctripPoi",
                'city_url': line['task']
            }
            #print(line['task'])
            it.insert_task(args)