#!/usr/bin/env python
# -*- coding: utf-8 -*-
from data_source import MysqlSource
from city.config import base_path,OpCity_config,config
from MongoTask.MongoTaskInsert import InsertTask
from collections import defaultdict
from my_logger import get_logger
from call_city_project.step_status import modify_status
import sys
import datetime
import pymysql
import json
import traceback


param = sys.argv[1]
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step8', path)


spider_data_base_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report8=%s,step8=%s where id=%s"
    update_sql_after = "update city_order set step9=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def get_tasks():
    query_sql = '''SELECT uid FROM hotel ORDER BY uid;'''

    for _l in MysqlSource(db_config=spider_data_base_data_config,
                          table_or_query=query_sql,
                          size=10000, is_table=False,
                          is_dict_cursor=False):
        yield _l[0]

def start_task():
    logger.info('[step8][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        task_name = 'merge_hotel_image_' + datetime.datetime.now().strftime('%Y%m%d_')+param
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
            save_collection_names = it.generate_collection_name(), task_name

        tasks = modify_status('step8', param, save_collection_names)
        logger.info('[step8][%s] tasks: %s' % (param, str(tasks)))

        # update_step_report('', param, 1, 0)
        logger.info('[step8][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        logger.info('[step8][%s]======== failed =======' % (return_result,))


if __name__ == '__main__':
    start_task()
