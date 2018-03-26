#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午6:52
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_qyer_city_sugg.py
# @Software: PyCharm
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from my_logger import get_logger
from service_platform_conn_pool import verify_info_new_pool, fetchall
import pymysql

logger = get_logger("insert_mongo_task")

def get_tasks():
    conn = pymysql.connect(host='10.10.230.206', user='mioji_admin', password='mioji1109', charset='utf8',
                           db='source_info')
    cursor = conn.cursor()
    sql = "select suggest, sid, city_id, country_id, s_city, s_country from ota_location where label_batch = '20180320a' and source = 'gha';"
    cursor.execute(sql, ())
    result = cursor.fetchall()
    print(cursor.rowcount)
    cursor.close()
    conn.close()
    for line in result:
        yield line


if __name__ == '__main__':
    source = 'gha'
    task_name = 'city_hotel_{}_20180325a'.format(source)
    with InsertTask(worker='proj.total_tasks.hotel_list_task', queue='hotel_list', routine_key='hotel_list',
                    task_name=task_name, source=source.title(), _type='HotelList',
                    priority=3, task_type=TaskType.CITY_TASK) as it:
        for suggest, source_id, city_id, country_id, s_city, s_country in get_tasks():

            s_city = s_city if s_city!='NULL' else ''
            print(s_city)
            args = {
                'source': 'gha',
                'source_id': source_id,
                'suggest': str(source_id)+'&'+s_city+'&'+s_country,
                'city_id': city_id,
                'country_id': country_id
            }
            it.insert_task(args)
        print(it.generate_collection_name(), task_name)
