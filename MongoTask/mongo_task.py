#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 上午8:19
# @Author  : Hou Rong
# @Site    :
# @File    : mongo_task.py
# @Software: PyCharm
import pymongo
import datetime
import json
import hashlib
import pymysql
import toolbox.Date

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['RoutineTask']

# conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='shop_merge')
# conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='rest_merge')
conn = pymysql.connect(host='10.19.118.147', user='reader', password='mioji1109', charset='utf8', db='source_info')

cursor = conn.cursor()
if __name__ == '__main__':
    # cursor.execute('''select distinct id,url from attr where source='daodao';''')
    # cursor.execute('''select distinct id,url from shop where source='daodao' limit 1;''')
    cursor.execute('''SELECT
  source,
  city_id
FROM hotel_suggestions_city
WHERE annotation != -1 AND select_index != -1 AND source IN ('booking', 'hotels', 'expedia', 'elong', 'agoda');''')
    data = []
    _count = 0
    _finished = 0
    exc_set = set()
    for source, city_id in cursor.fetchall():
        for check_in in toolbox.Date.date_takes(90, 10, 20):
            _count += 1
            task_info = {
                # 'worker': 'proj.qyer_poi_tasks.qyer_poi_task',
                # 'worker': 'proj.tasks.get_lost_attr',
                # 'worker': 'proj.tasks.get_lost_shop',
                # 'worker': 'proj.tasks.get_lost_rest',
                'worker': 'proj.hotel_list_routine_tasks.hotel_routine_list_task',
                # 'queue': 'poi_task_1',
                # 'routing_key': 'poi_task_1',
                'queue': 'hotel_list_task',
                'routing_key': 'hotel_list_task',
                'task_name': 'hotel_list_task_{0}'.format(source),
                # 'task_name': 'daodao_rest',
                'args': {
                    'source': source,
                    'city_id': city_id,
                    'check_in': check_in
                },
                'priority': 10,
                'running': 0,
                # 'finished': 0,
                # 'used_times': 0,
                'utime': datetime.datetime.now()
            }
            task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
            try:
                collections.save(task_info)
                _finished += 1
            except Exception as exc:
                # exc_set.add()
                pass
            if _count % 1000 == 0:
                print("Now", _count)
                print("Finished", _finished)
    print(exc_set)
    #     data.append(task_info)
    #     if _count % 10000 == 0:
    #         print(_count)
    #         try:
    #             collections.insert(data, continue_on_error=True)
    #             data = []
    #         except Exception:
    #             pass
    #
    # try:
    #     collections.insert(data, continue_on_error=True)
    # except Exception:
    #     pass
