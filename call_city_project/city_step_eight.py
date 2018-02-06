#!/usr/bin/env python
# -*- coding: utf-8 -*-
from data_source import MysqlSource
from my_logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask
import datetime

logger = get_logger("insert_mongo_task")

spider_data_base_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


def get_tasks():
    query_sql = '''SELECT uid
FROM hotel
ORDER BY uid;'''

    for _l in MysqlSource(db_config=spider_data_base_data_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=False):
        yield _l[0]


if __name__ == '__main__':
    task_name = 'merge_hotel_image_' + datetime.datetime.now().strftime('%Y%m%d_%H')
    with InsertTask(worker='proj.total_tasks.hotel_img_merge_task', queue='merge_task', routine_key='merge_task',
                    task_name=task_name, source='Any', _type='HotelImgMerge',
                    priority=11) as it:
        for uid in get_tasks():
            args = {
                'uid': uid,
                'min_pixels': '200000',
                'target_table': 'hotel'
            }
            it.insert_task(args)
