#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
from MongoTask.MongoTaskInsert import InsertTask, TaskType

config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'source_info',
    'charset': 'utf8'
}

google_config = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'mioji1109',
    'db': 'base_data',
    'charset': 'utf8'
}
def create_task():
    conn = pymysql.connect(**config)
    daodao_sql = "select suggest from ota_location where source = 'daodao'"
    google_sql = "select distinct(hotel_name),hotel_name_en from base_data.hotel"
    cursor = conn.cursor()
    cursor.execute(daodao_sql)
    daodao_values = cursor.fetchall()
    conn = pymysql.connect(**google_config)
    cursor = conn.cursor()
    cursor.execute(google_sql)
    google_values = cursor.fetchall()
    with InsertTask(worker='proj.total_tasks.other_source_hotel_url', queue='supplement_field', routine_key='supplement_field',
                    task_name='daodao_hotel_url_20180329a', source='daodao', _type='daodaoHotel',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for value in daodao_values[:1000]:
            url = value[0]
            url = url.replace('Tourism','Hotels').replace('Vacations','Hotels')
            args = {
                'url': url,
                'source': 'daodao',
                'spider_tag': 'daodaoListHotel',
                'data_from': 'daodao'

            }
            it.insert_task(args)
        for value in google_values[:1000]:
            hotel_name = value[0]
            hotel_name_en = value[1]
            key_word = ','.join([hotel_name,hotel_name_en])
            args = {
                'keyword': key_word,
                'source': 'daodao',
                'spider_tag': '',
                'data_from': 'google'
            }
            it.insert_task(args)

if __name__ == "__main__":
    create_task()